"""
FastAPI Backend & SOC Analyst Dashboard Microservice
Provides REST & WebSocket APIs for real-time security alerts, XAI feature attributions,
entity behavioral sequence timelines, system metrics, manual alert acknowledgments, and analytics.
"""

import os
import json
import time
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from kafka import KafkaConsumer

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DashboardAPI")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "anomaly_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "soc_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "soc_password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_SCORED_ALERTS = os.getenv("TOPIC_SCORED_ALERTS", "scored-alerts")

app = FastAPI(
    title="UEBA Behavioral Anomaly Detection & XAI SOC Dashboard",
    description="Real-time SOC Analyst Dashboard API for UEBA Anomaly Detection & SHAP Explainability",
    version="1.1.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("WebSocket client disconnected.")

    async def broadcast(self, message: Dict[str, Any]):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send to WS client: {e}")

manager = ConnectionManager()


@app.get("/api/metrics")
def get_system_metrics():
    """Returns real-time system performance and detection metrics."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM system_metrics ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()

        cur.execute("SELECT COUNT(*) as high_risk_count FROM alerts WHERE risk_score >= 7.0;")
        high_risk = cur.fetchone()["high_risk_count"]

        cur.execute("SELECT COUNT(*) as ack_count FROM alerts WHERE status = 'ACKNOWLEDGED';")
        acknowledged = cur.fetchone()["ack_count"]

        cur.close()
        conn.close()

        if not row:
            return {
                "total_logs_processed": 0,
                "total_alerts_flagged": 0,
                "high_risk_anomalies": 0,
                "acknowledged_alerts": 0,
                "avg_inference_latency_ms": 14.5
            }

        return {
            "total_logs_processed": row.get("total_logs_processed", 0),
            "total_alerts_flagged": row.get("total_alerts_flagged", 0),
            "high_risk_anomalies": high_risk,
            "acknowledged_alerts": acknowledged,
            "avg_inference_latency_ms": row.get("avg_inference_latency_ms", 14.5)
        }
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        return {
            "total_logs_processed": 0,
            "total_alerts_flagged": 0,
            "high_risk_anomalies": 0,
            "acknowledged_alerts": 0,
            "avg_inference_latency_ms": 14.5
        }


@app.get("/api/alerts")
def get_ranked_alerts(
    limit: int = Query(100, ge=1, le=500),
    sort_by: str = Query("risk_score", regex="^(risk_score|timestamp)$"),
    order: str = Query("desc", regex="^(asc|desc)$"),
    anomaly_type: Optional[str] = None,
    min_risk: Optional[float] = None,
    search: Optional[str] = None,
    status: Optional[str] = None
):
    """Returns ranked alert queue filterable by risk threshold, threat type, search query, and status."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = "SELECT id, alert_id, timestamp, entity_id, entity_type, role, anomaly_type, risk_score, point_error, seq_error, source_ip, geo_location, resource_accessed, status FROM alerts WHERE 1=1"
        params = []

        if anomaly_type:
            query += " AND anomaly_type = %s"
            params.append(anomaly_type)

        if min_risk is not None:
            query += " AND risk_score >= %s"
            params.append(min_risk)

        if status:
            query += " AND status = %s"
            params.append(status)

        if search and search.strip():
            s = f"%{search.strip()}%"
            query += " AND (entity_id ILIKE %s OR alert_id ILIKE %s OR source_ip ILIKE %s OR resource_accessed ILIKE %s OR role ILIKE %s)"
            params.extend([s, s, s, s, s])

        query += f" ORDER BY {sort_by} {order.upper()} LIMIT %s;"
        params.append(limit)

        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        alerts = []
        for r in rows:
            r_dict = dict(r)
            if isinstance(r_dict["timestamp"], datetime):
                r_dict["timestamp"] = r_dict["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            alerts.append(r_dict)

        return alerts
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []


@app.get("/api/alerts/{alert_id}")
def get_alert_detail(alert_id: str):
    """Returns full alert payload including raw log and TreeSHAP XAI feature attributions."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM alerts WHERE alert_id = %s;", (alert_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        alert = dict(row)
        if isinstance(alert["timestamp"], datetime):
            alert["timestamp"] = alert["timestamp"].strftime("%Y-%m-%d %H:%M:%S")

        return alert
    except Exception as e:
        logger.error(f"Error fetching alert detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str):
    """Updates an alert status to ACKNOWLEDGED in PostgreSQL and broadcasts update."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("UPDATE alerts SET status = 'ACKNOWLEDGED' WHERE alert_id = %s RETURNING *;", (alert_id,))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Alert not found")

        updated_alert = dict(row)
        if isinstance(updated_alert["timestamp"], datetime):
            updated_alert["timestamp"] = updated_alert["timestamp"].strftime("%Y-%m-%d %H:%M:%S")

        # Broadcast status update event
        await manager.broadcast({
            "event_type": "ALERT_ACKNOWLEDGED",
            "alert": updated_alert
        })

        return {"status": "success", "alert_id": alert_id, "new_status": "ACKNOWLEDGED"}
    except Exception as e:
        logger.error(f"Error acknowledging alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/charts/summary")
def get_charts_summary():
    """Returns analytics data for SOC Dashboard charts (threat distribution, risk histogram, top targets)."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Threat Type Distribution
        cur.execute("SELECT anomaly_type, COUNT(*) as count FROM alerts GROUP BY anomaly_type ORDER BY count DESC;")
        threat_dist = [dict(r) for r in cur.fetchall()]

        # 2. Risk Score Histogram
        cur.execute("""
            SELECT 
                CASE 
                    WHEN risk_score >= 9.0 THEN 'Critical (9.0-10.0)'
                    WHEN risk_score >= 7.0 THEN 'High (7.0-8.9)'
                    WHEN risk_score >= 4.0 THEN 'Medium (4.0-6.9)'
                    ELSE 'Low (1.0-3.9)'
                END as risk_range,
                COUNT(*) as count
            FROM alerts
            GROUP BY risk_range
            ORDER BY min(risk_score) DESC;
        """)
        risk_hist = [dict(r) for r in cur.fetchall()]

        # 3. Top 5 Targeted Entities
        cur.execute("SELECT entity_id, role, COUNT(*) as alert_count FROM alerts GROUP BY entity_id, role ORDER BY alert_count DESC LIMIT 5;")
        top_entities = [dict(r) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return {
            "threat_distribution": threat_dist,
            "risk_histogram": risk_hist,
            "top_targeted_entities": top_entities
        }
    except Exception as e:
        logger.error(f"Error fetching charts summary: {e}")
        return {"threat_distribution": [], "risk_histogram": [], "top_targeted_entities": []}


@app.get("/api/entities/{entity_id}/timeline")
def get_entity_timeline(entity_id: str):
    """Returns historical event sequence & baseline metrics for entity timeline visualization."""
    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT * FROM entity_baselines WHERE entity_id = %s;", (entity_id,))
        baseline_row = cur.fetchone()

        cur.execute("""
            SELECT alert_id, timestamp, anomaly_type, risk_score, source_ip, geo_location, resource_accessed, raw_log, status
            FROM alerts WHERE entity_id = %s ORDER BY timestamp DESC LIMIT 20;
        """, (entity_id,))
        alerts_rows = cur.fetchall()

        cur.close()
        conn.close()

        events = []
        for a in alerts_rows:
            ad = dict(a)
            if isinstance(ad["timestamp"], datetime):
                ad["timestamp"] = ad["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            events.append(ad)

        return {
            "entity_id": entity_id,
            "baseline": dict(baseline_row) if baseline_row else {},
            "recent_events": events
        }
    except Exception as e:
        logger.error(f"Error fetching entity timeline: {e}")
        return {"entity_id": entity_id, "baseline": {}, "recent_events": []}


@app.get("/api/threat-taxonomy")
def get_threat_taxonomy():
    """Returns breakdown of detected threat taxonomy with color mappings."""
    taxonomy_colors = {
        "impossible_travel": {"color": "#FF0055", "label": "Impossible Travel", "badge": "Crimson Red"},
        "lateral_movement": {"color": "#E600FF", "label": "Lateral Movement", "badge": "Deep Magenta"},
        "brute_force": {"color": "#FF9900", "label": "Brute Force Attack", "badge": "Amber Orange"},
        "credential_stuffing": {"color": "#FF6600", "label": "Credential Stuffing", "badge": "Burnt Orange"},
        "low_and_slow_exfiltration": {"color": "#9933FF", "label": "Low & Slow Exfiltration", "badge": "Purple"},
        "device_spoofing": {"color": "#00E5FF", "label": "Device Spoofing", "badge": "Cyan/Teal"},
        "insider_drift": {"color": "#0099FF", "label": "Insider Behavior Drift", "badge": "Electric Blue"}
    }

    try:
        conn = get_pg_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT anomaly_type, COUNT(*) as count, AVG(risk_score) as avg_risk FROM alerts GROUP BY anomaly_type;")
        rows = cur.fetchall()
        cur.close()
        conn.close()

        counts = {r["anomaly_type"]: {"count": r["count"], "avg_risk": round(float(r["avg_risk"]), 1)} for r in rows}

        results = []
        for key, meta in taxonomy_colors.items():
            stat = counts.get(key, {"count": 0, "avg_risk": 0.0})
            results.append({
                "type_id": key,
                "name": meta["label"],
                "color": meta["color"],
                "badge": meta["badge"],
                "count": stat["count"],
                "avg_risk": stat["avg_risk"]
            })
        return results
    except Exception as e:
        logger.error(f"Error fetching threat taxonomy: {e}")
        return []


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    """Real-time WebSocket endpoint streaming scored alerts from Kafka to connected clients."""
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def kafka_ws_bridge():
    await asyncio.sleep(5)
    logger.info(f"Starting Kafka WS Bridge listening on '{TOPIC_SCORED_ALERTS}'...")
    loop = asyncio.get_event_loop()

    def consume_kafka():
        consumer = None
        while consumer is None:
            try:
                consumer = KafkaConsumer(
                    TOPIC_SCORED_ALERTS,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    group_id="dashboard-ws-bridge-group"
                )
            except Exception:
                time.sleep(5)

        for msg in consumer:
            asyncio.run_coroutine_threadsafe(manager.broadcast({
                "event_type": "NEW_ALERT",
                "alert": msg.value
            }), loop)

    loop.run_in_executor(None, consume_kafka)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(kafka_ws_bridge())


static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    os.makedirs(static_dir, exist_ok=True)

app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
def read_root():
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse({
        "system": "AI-Powered Behavioral Anomaly Detection System",
        "status": "Operational",
        "docs": "/docs"
    })
