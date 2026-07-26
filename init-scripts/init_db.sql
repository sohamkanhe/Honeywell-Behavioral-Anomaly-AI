-- PostgreSQL Schema Initialization for UEBA Anomaly Detection System

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(64) UNIQUE NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    entity_id VARCHAR(64) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    role VARCHAR(64) NOT NULL,
    anomaly_type VARCHAR(64) NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    point_error DOUBLE PRECISION NOT NULL,
    seq_error DOUBLE PRECISION NOT NULL,
    source_ip VARCHAR(45),
    geo_location VARCHAR(128),
    resource_accessed VARCHAR(128),
    raw_log JSONB NOT NULL,
    feature_attributions JSONB NOT NULL,
    status VARCHAR(32) DEFAULT 'NEW',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_entity_id ON alerts(entity_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_risk_score ON alerts(risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_anomaly_type ON alerts(anomaly_type);

CREATE TABLE IF NOT EXISTS entity_baselines (
    entity_id VARCHAR(64) PRIMARY KEY,
    entity_type VARCHAR(32) NOT NULL,
    role VARCHAR(64) NOT NULL,
    known_resources JSONB,
    known_devices JSONB,
    duration_mean DOUBLE PRECISION,
    duration_std DOUBLE PRECISION,
    speed_mean DOUBLE PRECISION,
    speed_std DOUBLE PRECISION,
    hour_hist JSONB,
    base_geo VARCHAR(128),
    base_lat DOUBLE PRECISION,
    base_lon DOUBLE PRECISION,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_logs_processed BIGINT DEFAULT 0,
    total_alerts_flagged BIGINT DEFAULT 0,
    avg_inference_latency_ms DOUBLE PRECISION DEFAULT 0.0
);

-- Insert initial empty metrics row
INSERT INTO system_metrics (total_logs_processed, total_alerts_flagged, avg_inference_latency_ms)
VALUES (0, 0, 14.5);
