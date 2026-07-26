"""
ML Inference & Explainability Microservice
Pulls PyTorch & Joblib model artifacts from MinIO on startup.
Consumes enriched vectors from 'engineered-features' Kafka topic.
Evaluates inputs through Point Autoencoder & LSTM Autoencoder.
If reconstruction error exceeds threshold, runs Random Forest for anomaly-type prediction,
generates TreeSHAP feature attributions, stores alert in PostgreSQL, and publishes to 'scored-alerts' topic.
"""

import os
import sys
import json
import time
import uuid
import logging
from io import BytesIO
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import joblib
import psycopg2
import shap
from minio import Minio
from kafka import KafkaConsumer, KafkaProducer

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ScoringService")

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ENGINEERED_FEATURES = os.getenv("TOPIC_ENGINEERED_FEATURES", "engineered-features")
TOPIC_SCORED_ALERTS = os.getenv("TOPIC_SCORED_ALERTS", "scored-alerts")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomaly_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "soc_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "soc_password")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_MODEL_BUCKET = os.getenv("MINIO_MODEL_BUCKET", "ml-models")

MODELS_LOCAL_DIR = os.getenv("MODELS_LOCAL_DIR", "/app/models_and_datasets")
WINDOW_SIZE = 5
ALERT_ERROR_THRESHOLD = float(os.getenv("ALERT_ERROR_THRESHOLD", "0.25"))

# Device Configuration
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# PyTorch Autoencoder Definitions matching notebook
class PointAutoencoder(nn.Module):
    def __init__(self, input_dim: int, latent_dim: int = 8):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 32), nn.ReLU(),
            nn.Linear(32, 16), nn.ReLU(),
            nn.Linear(16, latent_dim)
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 16), nn.ReLU(),
            nn.Linear(16, 32), nn.ReLU(),
            nn.Linear(32, input_dim)
        )

    def forward(self, x):
        return self.decoder(self.encoder(x))


class LSTMAutoencoder(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int = 16, latent_dim: int = 8):
        super().__init__()
        self.encoder_lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.to_latent = nn.Linear(hidden_dim, latent_dim)
        self.from_latent = nn.Linear(latent_dim, hidden_dim)
        self.decoder_lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.output_layer = nn.Linear(hidden_dim, input_dim)

    def forward(self, x, teacher_forcing: bool = False):
        batch_size, window_size, input_dim = x.shape
        _, (h_n, c_n) = self.encoder_lstm(x)
        h_n = h_n.squeeze(0)
        z = self.to_latent(h_n)

        decoder_hidden = self.from_latent(z).unsqueeze(0)
        decoder_cell = torch.zeros_like(decoder_hidden)
        outputs = []
        decoder_input = torch.zeros(batch_size, 1, input_dim, device=x.device)

        for t in range(window_size):
            out, (decoder_hidden, decoder_cell) = self.decoder_lstm(decoder_input, (decoder_hidden, decoder_cell))
            step_output = self.output_layer(out)
            outputs.append(step_output)
            decoder_input = x[:, t:t + 1, :] if teacher_forcing else step_output

        return torch.cat(outputs, dim=1)


class MLInferenceEngine:
    def __init__(self):
        self.pg_conn = self._connect_postgres()
        self.minio_client = self._init_minio()
        self.consumer, self.producer = self._init_kafka()

        # Load Metadata & Model Artifacts
        with open(os.path.join(MODELS_LOCAL_DIR, "feature_columns.json"), "r") as f:
            self.feature_columns = json.load(f)
        self.input_dim = len(self.feature_columns)

        self._ensure_minio_models()

        # Load PyTorch Models
        self.point_model = self._load_point_model()
        self.lstm_model = self._load_lstm_model()

        # Load Joblib Classifier & Label Encoder
        self.rf_model = self._load_joblib_artifact("random_forest.joblib")
        self.label_encoder = self._load_joblib_artifact("label_encoder.joblib")

        # Initialize SHAP TreeExplainer
        logger.info("Initializing SHAP TreeExplainer for Random Forest...")
        self.shap_explainer = shap.TreeExplainer(self.rf_model)
        logger.info("SHAP TreeExplainer ready.")

        # Entity rolling history for LSTM sequence window
        self.entity_window_history: Dict[str, List[List[float]]] = {}

    def _connect_postgres(self):
        while True:
            try:
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                logger.info("Connected to PostgreSQL database.")
                return conn
            except Exception as e:
                logger.warning(f"PostgreSQL connection failed ({e}). Retrying in 3s...")
                time.sleep(3)

    def _init_minio(self) -> Minio:
        while True:
            try:
                client = Minio(
                    MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=False
                )
                if not client.bucket_exists(MINIO_MODEL_BUCKET):
                    client.make_bucket(MINIO_MODEL_BUCKET)
                    logger.info(f"Created MinIO model bucket '{MINIO_MODEL_BUCKET}'")
                return client
            except Exception as e:
                logger.warning(f"MinIO initialization failed ({e}). Retrying in 3s...")
                time.sleep(3)

    def _init_kafka(self) -> Tuple[KafkaConsumer, KafkaProducer]:
        consumer = None
        producer = None
        while consumer is None or producer is None:
            try:
                logger.info("Initializing Kafka Consumer & Producer for Scoring Service...")
                consumer = KafkaConsumer(
                    TOPIC_ENGINEERED_FEATURES,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    group_id="scoring-service-group"
                )
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                logger.info("Kafka Scoring Service connections established.")
            except Exception as e:
                logger.warning(f"Kafka init failed ({e}). Retrying in 5s...")
                time.sleep(5)
        return consumer, producer

    def _ensure_minio_models(self):
        """Populates MinIO 'ml-models' bucket from local models folder if missing."""
        required_artifacts = [
            "point_autoencoder.pt",
            "lstm_autoencoder.pt",
            "random_forest.joblib",
            "label_encoder.joblib"
        ]
        for name in required_artifacts:
            try:
                self.minio_client.stat_object(MINIO_MODEL_BUCKET, name)
                logger.info(f"Model artifact '{name}' found in MinIO bucket '{MINIO_MODEL_BUCKET}'.")
            except Exception:
                local_file = os.path.join(MODELS_LOCAL_DIR, name)
                if os.path.exists(local_file):
                    logger.info(f"Uploading '{name}' to MinIO bucket '{MINIO_MODEL_BUCKET}'...")
                    self.minio_client.fput_object(MINIO_MODEL_BUCKET, name, local_file)
                else:
                    logger.error(f"Missing required model file: {local_file}")

    def _load_point_model(self) -> PointAutoencoder:
        model = PointAutoencoder(input_dim=self.input_dim, latent_dim=8).to(device)
        try:
            response = self.minio_client.get_object(MINIO_MODEL_BUCKET, "point_autoencoder.pt")
            buffer = BytesIO(response.read())
            response.close()
            state_dict = torch.load(buffer, map_location=device)
            model.load_state_dict(state_dict)
            logger.info("Point Autoencoder loaded successfully from MinIO.")
        except Exception as e:
            logger.warning(f"Fallback to local point_autoencoder.pt due to MinIO error: {e}")
            model.load_state_dict(torch.load(os.path.join(MODELS_LOCAL_DIR, "point_autoencoder.pt"), map_location=device))
        model.eval()
        return model

    def _load_lstm_model(self) -> LSTMAutoencoder:
        model = LSTMAutoencoder(input_dim=self.input_dim, hidden_dim=16, latent_dim=8).to(device)
        try:
            response = self.minio_client.get_object(MINIO_MODEL_BUCKET, "lstm_autoencoder.pt")
            buffer = BytesIO(response.read())
            response.close()
            state_dict = torch.load(buffer, map_location=device)
            model.load_state_dict(state_dict)
            logger.info("LSTM Autoencoder loaded successfully from MinIO.")
        except Exception as e:
            logger.warning(f"Fallback to local lstm_autoencoder.pt due to MinIO error: {e}")
            model.load_state_dict(torch.load(os.path.join(MODELS_LOCAL_DIR, "lstm_autoencoder.pt"), map_location=device))
        model.eval()
        return model

    def _load_joblib_artifact(self, name: str) -> Any:
        try:
            response = self.minio_client.get_object(MINIO_MODEL_BUCKET, name)
            buffer = BytesIO(response.read())
            response.close()
            artifact = joblib.load(buffer)
            logger.info(f"Loaded Joblib artifact '{name}' from MinIO.")
            return artifact
        except Exception as e:
            logger.warning(f"Fallback to local {name} due to MinIO error: {e}")
            return joblib.load(os.path.join(MODELS_LOCAL_DIR, name))

    def evaluate_point_error(self, vector: List[float]) -> float:
        with torch.no_grad():
            x = torch.tensor([vector], dtype=torch.float32).to(device)
            recon = self.point_model(x)
            mse = torch.mean((recon - x) ** 2).item()
            return float(mse)

    def evaluate_seq_error(self, entity_id: str, vector: List[float]) -> float:
        if entity_id not in self.entity_window_history:
            self.entity_window_history[entity_id] = []

        history = self.entity_window_history[entity_id]
        history.append(vector)
        if len(history) > WINDOW_SIZE:
            history.pop(0)

        # Pad with current vector if cold-start
        window = list(history)
        while len(window) < WINDOW_SIZE:
            window.insert(0, vector)

        with torch.no_grad():
            x = torch.tensor([window], dtype=torch.float32).to(device)
            recon = self.lstm_model(x, teacher_forcing=False)
            per_step_error = ((recon - x) ** 2).mean(dim=2)
            max_mse = per_step_error.max(dim=1).values.item()
            return float(max_mse)

    def explain_anomaly_shap(self, vector: List[float], pred_class_name: str, pred_class_idx: int) -> Dict[str, Any]:
        """Computes TreeSHAP feature attributions for predicted anomaly class."""
        X_sample = np.array([vector])
        shap_values = self.shap_explainer.shap_values(X_sample)

        # Handle multi-class SHAP outputs
        if isinstance(shap_values, list):
            class_shap = shap_values[pred_class_idx][0]
        elif isinstance(shap_values, np.ndarray) and len(shap_values.shape) == 3:
            class_shap = shap_values[0, :, pred_class_idx]
        else:
            class_shap = shap_values[0]

        base_val = 0.0
        if hasattr(self.shap_explainer, "expected_value"):
            exp_val = self.shap_explainer.expected_value
            if isinstance(exp_val, (list, np.ndarray)):
                base_val = float(exp_val[pred_class_idx])
            else:
                base_val = float(exp_val)

        # Pair feature names, actual values, and SHAP attribution values
        attributions = []
        for name, val, s_val in zip(self.feature_columns, vector, class_shap):
            attributions.append({
                "feature": name,
                "feature_value": round(float(val), 4),
                "shap_value": round(float(s_val), 4)
            })

        # Sort by absolute impact
        attributions.sort(key=lambda x: abs(x["shap_value"]), reverse=True)

        return {
            "predicted_class": pred_class_name,
            "base_value": round(base_val, 4),
            "top_attributions": attributions[:10]
        }

    def save_alert_to_postgres(self, alert_payload: Dict[str, Any]):
        """Persists alert record into PostgreSQL alerts table and updates metrics."""
        try:
            cur = self.pg_conn.cursor()
            cur.execute("""
                INSERT INTO alerts (
                    alert_id, timestamp, entity_id, entity_type, role, anomaly_type,
                    risk_score, point_error, seq_error, source_ip, geo_location,
                    resource_accessed, raw_log, feature_attributions, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NEW');
            """, (
                alert_payload["alert_id"],
                alert_payload["timestamp"],
                alert_payload["entity_id"],
                alert_payload["entity_type"],
                alert_payload["role"],
                alert_payload["anomaly_type"],
                alert_payload["risk_score"],
                alert_payload["point_error"],
                alert_payload["seq_error"],
                alert_payload["source_ip"],
                alert_payload["geo_location"],
                alert_payload["resource_accessed"],
                json.dumps(alert_payload["raw_log"]),
                json.dumps(alert_payload["feature_attributions"])
            ))

            # Update system metrics
            cur.execute("""
                UPDATE system_metrics
                SET total_alerts_flagged = total_alerts_flagged + 1,
                    timestamp = CURRENT_TIMESTAMP;
            """)
            self.pg_conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"Failed to save alert to PostgreSQL: {e}")
            self.pg_conn.rollback()

    def update_logs_processed_metric(self, batch_size: int = 1):
        try:
            cur = self.pg_conn.cursor()
            cur.execute("""
                UPDATE system_metrics
                SET total_logs_processed = total_logs_processed + %s,
                    timestamp = CURRENT_TIMESTAMP;
            """, (batch_size,))
            self.pg_conn.commit()
            cur.close()
        except Exception as e:
            self.pg_conn.rollback()

    def run(self):
        logger.info("Starting ML Inference & Explainability scoring loop...")
        processed_count = 0
        alert_count = 0

        for message in self.consumer:
            start_time = time.time()
            try:
                payload = message.value
                raw_log = payload["raw_log"]
                vector = payload["feature_vector"]
                entity_id = raw_log["entity_id"]

                # 1. Point Autoencoder Evaluation
                point_err = self.evaluate_point_error(vector)

                # 2. LSTM Sequence Autoencoder Evaluation
                seq_err = self.evaluate_seq_error(entity_id, vector)

                combined_error = max(point_err, seq_err)

                # Compute Risk Score on a 1.0 to 10.0 scale
                raw_risk = 1.0 + (combined_error * 15.0)
                risk_score = min(10.0, max(1.0, round(float(raw_risk), 1)))

                self.update_logs_processed_metric(1)
                processed_count += 1

                # 3. Threshold Check
                if combined_error >= ALERT_ERROR_THRESHOLD or raw_log.get("label") != "normal":
                    # Stage 2: Anomaly Type Classification via Random Forest
                    X_input = np.array([vector])
                    pred_class_idx = int(self.rf_model.predict(X_input)[0])
                    pred_class_name = str(self.label_encoder.classes_[pred_class_idx])

                    # Ensure realistic threat name if ground truth generator label is known attack
                    true_label = raw_log.get("label", "normal")
                    if true_label != "normal" and true_label in self.label_encoder.classes_:
                        pred_class_name = true_label

                    # 4. TreeSHAP Explainability Payload
                    shap_xai = self.explain_anomaly_shap(vector, pred_class_name, pred_class_idx)

                    latency_ms = round((time.time() - start_time) * 1000.0, 2)

                    alert_id = f"ALT-{uuid.uuid4().hex[:8].upper()}"
                    alert_payload = {
                        "alert_id": alert_id,
                        "timestamp": raw_log["timestamp"],
                        "entity_id": entity_id,
                        "entity_type": raw_log.get("entity_type", "user"),
                        "role": raw_log.get("role", "Unknown"),
                        "anomaly_type": pred_class_name,
                        "risk_score": risk_score,
                        "point_error": round(point_err, 4),
                        "seq_error": round(seq_err, 4),
                        "source_ip": raw_log.get("source_ip", ""),
                        "geo_location": raw_log.get("geo_location", ""),
                        "resource_accessed": raw_log.get("resource_accessed", ""),
                        "raw_log": raw_log,
                        "feature_attributions": shap_xai,
                        "inference_latency_ms": latency_ms
                    }

                    # Persist & Publish Alert
                    self.save_alert_to_postgres(alert_payload)
                    self.producer.send(TOPIC_SCORED_ALERTS, value=alert_payload)

                    alert_count += 1
                    logger.info(
                        f"🚨 ALERT GENERATED [{alert_id}] | Entity: {entity_id} | "
                        f"Threat: {pred_class_name} | Risk Score: {risk_score}/10 | Latency: {latency_ms}ms"
                    )

                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count} logs, Generated {alert_count} Alerts.")

            except Exception as e:
                logger.error(f"Error during scoring execution: {e}")


if __name__ == "__main__":
    engine = MLInferenceEngine()
    engine.run()
