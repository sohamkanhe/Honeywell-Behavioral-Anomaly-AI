"""
Stream Processing and Feature Engineering Microservice
Consumes 'raw-logs' from Kafka, retrieves entity baselines using Redis Cache-Aside pattern,
computes real-time behavioral features & deviation scores, archives raw data to MinIO cold data lake,
and publishes enriched payloads to 'engineered-features' Kafka topic.
"""

import os
import sys
import json
import time
import pickle
import logging
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from minio import Minio
from io import BytesIO
from kafka import KafkaConsumer, KafkaProducer

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("FeatureWorker")

# Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW_LOGS = os.getenv("TOPIC_RAW_LOGS", "raw-logs")
TOPIC_ENGINEERED_FEATURES = os.getenv("TOPIC_ENGINEERED_FEATURES", "engineered-features")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomaly_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "soc_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "soc_password")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_COLD_BUCKET = os.getenv("MINIO_COLD_BUCKET", "cold-lake")

BASELINES_PKL_PATH = os.getenv("BASELINES_PKL_PATH", "/app/models_and_datasets/baselines.pkl")
MEANS_CSV_PATH = os.getenv("MEANS_CSV_PATH", "/app/models_and_datasets/feature_means.csv")
STDS_CSV_PATH = os.getenv("STDS_CSV_PATH", "/app/models_and_datasets/feature_stds.csv")
COLUMNS_JSON_PATH = os.getenv("COLUMNS_JSON_PATH", "/app/models_and_datasets/feature_columns.json")


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Computes great-circle distance between two GPS coordinates in kilometers."""
    try:
        R = 6371.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2.0) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2.0) ** 2
        return 2.0 * R * atan2(sqrt(a), sqrt(1.0 - a))
    except Exception:
        return 0.0


class FeatureEngineeringWorker:
    def __init__(self):
        self.redis_client = self._connect_redis()
        self.pg_conn = self._connect_postgres()
        self.minio_client = self._init_minio()
        self.consumer, self.producer = self._init_kafka()

        # Load Feature Metadata
        self.feature_means = self._load_csv_dict(MEANS_CSV_PATH)
        self.feature_stds = self._load_csv_dict(STDS_CSV_PATH)
        with open(COLUMNS_JSON_PATH, "r") as f:
            self.feature_columns = json.load(f)

        # Load Baselines Fallback
        self.baselines_file_data = {}
        if os.path.exists(BASELINES_PKL_PATH):
            with open(BASELINES_PKL_PATH, "rb") as f:
                self.baselines_file_data = pickle.load(f)
            logger.info("Loaded baselines.pkl file.")

        self._seed_postgres_baselines()

    def _connect_redis(self) -> redis.Redis:
        while True:
            try:
                r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
                r.ping()
                logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
                return r
            except Exception as e:
                logger.warning(f"Redis connection failed ({e}). Retrying in 3s...")
                time.sleep(3)

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
                if not client.bucket_exists(MINIO_COLD_BUCKET):
                    client.make_bucket(MINIO_COLD_BUCKET)
                    logger.info(f"Created MinIO bucket '{MINIO_COLD_BUCKET}'")
                return client
            except Exception as e:
                logger.warning(f"MinIO initialization failed ({e}). Retrying in 3s...")
                time.sleep(3)

    def _init_kafka(self) -> Tuple[KafkaConsumer, KafkaProducer]:
        consumer = None
        producer = None
        while consumer is None or producer is None:
            try:
                logger.info("Initializing Kafka Consumer & Producer...")
                consumer = KafkaConsumer(
                    TOPIC_RAW_LOGS,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    group_id="feature-worker-group"
                )
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                logger.info("Kafka Consumer and Producer initialized successfully.")
            except Exception as e:
                logger.warning(f"Kafka init failed ({e}). Retrying in 5s...")
                time.sleep(5)
        return consumer, producer

    def _load_csv_dict(self, path: str) -> Dict[str, float]:
        if not os.path.exists(path):
            return {}
        df = pd.read_csv(path, index_col=0)
        return df.to_dict()["0"]

    def _seed_postgres_baselines(self):
        """Seeds entity_baselines table in PostgreSQL from baselines.pkl if empty."""
        try:
            cur = self.pg_conn.cursor()
            cur.execute("SELECT COUNT(*) FROM entity_baselines;")
            count = cur.fetchone()[0]
            if count == 0 and self.baselines_file_data:
                logger.info("Seeding PostgreSQL entity_baselines from baselines.pkl...")
                for entity_id, data in self.baselines_file_data.get("duration_stats_entity", {}).items():
                    role = "user"
                    etype = "user"
                    dur_mean = data.get("mean", 600.0)
                    dur_std = data.get("std", 100.0)
                    sp_stats = self.baselines_file_data.get("speed_stats_entity", {}).get(entity_id, {})
                    sp_mean = sp_stats.get("mean", 0.0)
                    sp_std = sp_stats.get("std", 1.0)
                    known_res = list(self.baselines_file_data.get("known_resources_entity", {}).get(entity_id, []))
                    known_dev = [list(pair) for pair in self.baselines_file_data.get("known_devices_entity", {}).get(entity_id, [])]
                    hour_hist = self.baselines_file_data.get("hour_hist_entity", {}).get(entity_id, {})

                    cur.execute("""
                        INSERT INTO entity_baselines (
                            entity_id, entity_type, role, known_resources, known_devices,
                            duration_mean, duration_std, speed_mean, speed_std, hour_hist
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (entity_id) DO NOTHING;
                    """, (
                        entity_id, etype, role, json.dumps(known_res), json.dumps(known_dev),
                        dur_mean, dur_std, sp_mean, sp_std, json.dumps(hour_hist)
                    ))
                self.pg_conn.commit()
                logger.info("PostgreSQL entity_baselines seeded successfully.")
            cur.close()
        except Exception as e:
            logger.error(f"Failed to seed postgres baselines: {e}")
            self.pg_conn.rollback()

    def get_entity_baseline_cache_aside(self, entity_id: str, role: str) -> Dict[str, Any]:
        """Retrieve entity baseline profile using Redis Cache-Aside pattern."""
        cache_key = f"baseline:{entity_id}"

        # 1. Try Redis cache
        try:
            cached = self.redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"Redis get error: {e}")

        # 2. Cache Miss: Fetch from PostgreSQL
        baseline = None
        try:
            cur = self.pg_conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT * FROM entity_baselines WHERE entity_id = %s;", (entity_id,))
            row = cur.fetchone()
            cur.close()
            if row:
                baseline = {
                    "known_resources": row["known_resources"] or [],
                    "known_devices": row["known_devices"] or [],
                    "duration_mean": row["duration_mean"] or 600.0,
                    "duration_std": row["duration_std"] or 100.0,
                    "speed_mean": row["speed_mean"] or 0.0,
                    "speed_std": row["speed_std"] or 1.0,
                    "hour_hist": row["hour_hist"] or {}
                }
        except Exception as e:
            logger.error(f"PostgreSQL fetch baseline error: {e}")
            self.pg_conn.rollback()

        # 3. Fallback to pickle or default if not found in PG
        if not baseline:
            dur_stats = self.baselines_file_data.get("duration_stats_entity", {}).get(entity_id, {})
            sp_stats = self.baselines_file_data.get("speed_stats_entity", {}).get(entity_id, {})
            baseline = {
                "known_resources": list(self.baselines_file_data.get("known_resources_entity", {}).get(entity_id, [])),
                "known_devices": [list(p) for p in self.baselines_file_data.get("known_devices_entity", {}).get(entity_id, [])],
                "duration_mean": dur_stats.get("mean", 600.0),
                "duration_std": dur_stats.get("std", 100.0),
                "speed_mean": sp_stats.get("mean", 0.0),
                "speed_std": sp_stats.get("std", 1.0),
                "hour_hist": self.baselines_file_data.get("hour_hist_entity", {}).get(entity_id, {})
            }

        # 4. Populate Redis with TTL (3600 seconds)
        try:
            self.redis_client.setex(cache_key, 3600, json.dumps(baseline))
        except Exception as e:
            logger.warning(f"Redis setex error: {e}")

        return baseline

    def compute_features(self, raw_log: Dict[str, Any]) -> Dict[str, Any]:
        """Calculates real-time behavioral features and deviation scores for raw log."""
        entity_id = raw_log["entity_id"]
        role = raw_log["role"]
        etype = raw_log["entity_type"]
        timestamp_dt = datetime.strptime(raw_log["timestamp"], "%Y-%m-%d %H:%M:%S")

        baseline = self.get_entity_baseline_cache_aside(entity_id, role)

        # 1. Command Novelty Score
        cmds = raw_log.get("command_sequence", [])
        if isinstance(cmds, str):
            cmds = json.loads(cmds)
        expected_cmds = set(self.baselines_file_data.get("expected_commands", {}).get(role, []))
        if len(cmds) == 0:
            cmd_novelty = 0.0
        else:
            novel_count = len(set(cmds) - expected_cmds)
            cmd_novelty = float(novel_count / len(cmds)) if expected_cmds else 0.0

        # 2. Resource Novelty Score
        res = raw_log.get("resource_accessed", "")
        known_res = set(baseline.get("known_resources", []))
        res_novelty = 0.0 if res in known_res else 1.0

        # 3. Device Novelty Score
        dev_fingerprint = raw_log.get("device_fingerprint", "")
        mac = raw_log.get("mac_address", "")
        known_devs = set([tuple(d) for d in baseline.get("known_devices", [])])
        dev_novelty = 0.0 if (dev_fingerprint, mac) in known_devs else 1.0

        # 4. Duration Z-Score
        duration = float(raw_log.get("session_duration_sec", 0))
        mean_dur = float(baseline.get("duration_mean", 600.0))
        std_dur = float(baseline.get("duration_std", 100.0))
        std_dur = std_dur if std_dur > 0 else 1.0
        duration_z = (duration - mean_dur) / std_dur

        # 5. Time Unusualness Score
        hour_hist = baseline.get("hour_hist", {})
        prob = float(hour_hist.get(str(timestamp_dt.hour), hour_hist.get(timestamp_dt.hour, 0.0)))
        time_unusualness = -float(np.log(prob + 1e-3))

        # 6. Geo-Velocity Z-Score (using Redis to store recent event state per entity)
        lat = float(raw_log.get("geo_lat", 0.0))
        lon = float(raw_log.get("geo_lon", 0.0))
        state_key = f"recent_state:{entity_id}"
        prev_state_json = None
        try:
            prev_state_json = self.redis_client.get(state_key)
        except Exception:
            pass

        geo_velocity_z = 0.0
        if prev_state_json:
            prev_state = json.loads(prev_state_json)
            prev_lat = float(prev_state["lat"])
            prev_lon = float(prev_state["lon"])
            prev_time = datetime.strptime(prev_state["timestamp"], "%Y-%m-%d %H:%M:%S")

            dist_km = haversine_km(prev_lat, prev_lon, lat, lon)
            time_gap_hr = max((timestamp_dt - prev_time).total_seconds() / 3600.0, 1e-6)
            implied_speed = dist_km / time_gap_hr

            sp_mean = float(baseline.get("speed_mean", 0.0))
            sp_std = float(baseline.get("speed_std", 1.0))
            sp_std = sp_std if sp_std > 0 else 1.0
            geo_velocity_z = (implied_speed - sp_mean) / sp_std

        # Update Redis recent state
        try:
            self.redis_client.setex(
                state_key, 86400,
                json.dumps({"lat": lat, "lon": lon, "timestamp": raw_log["timestamp"]})
            )
        except Exception:
            pass

        # 7. Cyclical Hour Features
        angle = 2 * np.pi * timestamp_dt.hour / 24.0
        hour_sin = float(np.sin(angle))
        hour_cos = float(np.cos(angle))

        # 8. Recent Failed Auth Count (5-minute rolling window stored in Redis sorted set)
        failed_key = f"fails:{entity_id}"
        now_ts = timestamp_dt.timestamp()
        five_min_ago = now_ts - 300.0

        failed_count = 0
        try:
            if raw_log.get("auth_result") == "fail":
                self.redis_client.zadd(failed_key, {str(now_ts): now_ts})
            self.redis_client.zremrangebyscore(failed_key, 0, five_min_ago)
            failed_count = self.redis_client.zcard(failed_key)
            self.redis_client.expire(failed_key, 600)
        except Exception as e:
            logger.warning(f"Redis sorted set error: {e}")

        failed_auth_log1p = float(np.log1p(failed_count))

        # Raw numeric feature vector
        raw_numeric = {
            "command_novelty_score": cmd_novelty,
            "resource_novelty_score": res_novelty,
            "device_novelty_score": dev_novelty,
            "duration_zscore": duration_z,
            "time_unusualness_score": time_unusualness,
            "geo_velocity_zscore": geo_velocity_z,
            "hour_sin": hour_sin,
            "hour_cos": hour_cos,
            "recent_failed_auth_count": failed_auth_log1p
        }

        # Standardize numeric features using feature_means.csv and feature_stds.csv
        scaled_numeric = {}
        for k, v in raw_numeric.items():
            mean_val = float(self.feature_means.get(k, 0.0))
            std_val = float(self.feature_stds.get(k, 1.0))
            std_val = std_val if std_val > 0 else 1.0
            scaled_numeric[k] = float((v - mean_val) / std_val)

        # Construct full feature vector matching feature_columns.json order
        full_vector = []
        feature_dict = {}

        for col in self.feature_columns:
            if col in scaled_numeric:
                val = scaled_numeric[col]
            elif col.startswith("role_"):
                role_name = col.replace("role_", "")
                val = 1.0 if role == role_name else 0.0
            elif col.startswith("etype_"):
                etype_name = col.replace("etype_", "")
                val = 1.0 if etype == etype_name else 0.0
            else:
                val = 0.0
            full_vector.append(val)
            feature_dict[col] = val

        return {
            "raw_log": raw_log,
            "raw_numeric": raw_numeric,
            "scaled_numeric": scaled_numeric,
            "feature_dict": feature_dict,
            "feature_vector": full_vector
        }

    def archive_to_cold_lake(self, raw_log: Dict[str, Any], feature_payload: Dict[str, Any]):
        """Uploads raw log and engineered features to MinIO cold data lake."""
        try:
            date_str = raw_log["timestamp"].split(" ")[0]
            entity_id = raw_log["entity_id"]
            object_name = f"year={date_str[:4]}/month={date_str[5:7]}/day={date_str[8:10]}/{entity_id}_{int(time.time()*1000)}.json"

            data = json.dumps({
                "raw_log": raw_log,
                "engineered_features": feature_payload["feature_dict"]
            }).encode("utf-8")

            self.minio_client.put_object(
                MINIO_COLD_BUCKET,
                object_name,
                BytesIO(data),
                len(data),
                content_type="application/json"
            )
        except Exception as e:
            logger.warning(f"MinIO cold lake archive failed ({e})")

    def run(self):
        logger.info("Starting Stream Processing & Feature Engineering loop...")
        processed_count = 0

        for message in self.consumer:
            try:
                raw_log = message.value
                feature_payload = self.compute_features(raw_log)

                # Push enriched payload to Kafka 'engineered-features' topic
                output_payload = {
                    "raw_log": raw_log,
                    "feature_vector": feature_payload["feature_vector"],
                    "feature_dict": feature_payload["feature_dict"],
                    "raw_numeric": feature_payload["raw_numeric"]
                }
                self.producer.send(TOPIC_ENGINEERED_FEATURES, value=output_payload)

                # Archive cold data to MinIO asynchronously / non-blocking
                self.archive_to_cold_lake(raw_log, feature_payload)

                processed_count += 1
                if processed_count % 50 == 0:
                    logger.info(f"Processed & Enriched {processed_count} logs -> topic '{TOPIC_ENGINEERED_FEATURES}'")

            except Exception as e:
                logger.error(f"Error processing log message: {e}")


if __name__ == "__main__":
    worker = FeatureEngineeringWorker()
    worker.run()
