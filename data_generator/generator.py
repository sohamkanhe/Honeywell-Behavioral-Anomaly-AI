"""
Synthetic Access Log Generator Microservice
Streams realistic access logs with habitual patterns and injected security attack scenarios
to the Apache Kafka 'raw-logs' topic in near real-time.

Configured for ~60 logs/minute with 2.5% weighted anomaly injection rate.
"""

import os
import sys
import json
import time
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("DataGenerator")

fake = Faker()

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW_LOGS = os.getenv("TOPIC_RAW_LOGS", "raw-logs")
STREAM_INTERVAL_SEC = float(os.getenv("STREAM_INTERVAL_SEC", "1.0"))  # 60 logs per minute
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.025"))  # 2.5% anomaly rate

ROLE_COUNTS = {
    "Software Engineer": 45,
    "Finance & Accounting": 15,
    "Security & IT Admin": 15,
    "Executive": 15,
    "Marketing & Sales": 25,
    "General Admin": 30,
    "Intern / New Hire": 15
}
NUM_SERVICE_ACCOUNTS = 20
NUM_EDGE_DEVICES = 20

ROLE_PROFILES: Dict[str, Dict[str, Any]] = {
    "Software Engineer": {
        "resources": ["git-repo-backend", "git-repo-frontend", "ci-cd-jenkins", "cpp-build-server", "python-package-registry"],
        "auth": ["ssh_key", "dev_certificate", "password_mfa"],
        "devices": ["macOS Monterey - Intel i7", "Ubuntu 22.04 LTS", "macOS Ventura - Intel i9"],
        "ip_pool": ["10.20.30.101", "10.20.30.102", "10.20.30.103", "10.20.30.104", "10.20.30.105"],
        "commands": [
            ["login", "clone_repo", "commit_code", "push_code", "logout"],
            ["login", "compile_cpp_code", "run_tests", "logout"],
            ["login", "start_spring_boot_server", "deploy_react_app", "logout"]
        ]
    },
    "Finance & Accounting": {
        "resources": ["payroll-db", "erp-corporate", "invoicing-portal", "treasury-dashboard"],
        "auth": ["hardware_key", "biometric", "password_mfa"],
        "devices": ["Windows 11 Enterprise - Corporate Build"],
        "ip_pool": ["10.40.10.11", "10.40.10.12", "10.40.10.13"],
        "commands": [
            ["login", "access_erp", "download_report", "logout"],
            ["login", "open_payroll", "approve_transaction", "logout"],
            ["login", "view_treasury_dashboard", "logout"]
        ]
    },
    "Security & IT Admin": {
        "resources": ["firewall-config-console", "iam-console", "network-control-plane", "packet-capture-store", "auth-server-logs"],
        "auth": ["certificate", "certificate_mfa", "biometric"],
        "devices": ["Hardened Linux Terminal", "Arch Linux Custom"],
        "ip_pool": ["10.5.0.1", "10.5.0.2", "10.5.0.3"],
        "commands": [
            ["login", "escalate_privilege", "restart_service", "logout"],
            ["login", "open_firewall_console", "modify_rules", "logout"],
            ["login", "capture_packet_logs", "export_pcap", "logout"]
        ]
    },
    "Executive": {
        "resources": ["board-strategy-docs", "exec-kpi-dashboard", "financial-summary-portal"],
        "auth": ["password_mfa", "password_sms", "biometric"],
        "devices": ["macOS Air - M2", "iPadOS Terminal", "iOS Mobile", "Windows 11 Executive"],
        "ip_pool": ["DYNAMIC"],
        "commands": [
            ["login", "view_dashboard", "download_summary", "logout"],
            ["login", "read_confidential_file", "logout"],
            ["login", "approve_strategy_doc", "logout"]
        ]
    },
    "Marketing & Sales": {
        "resources": ["crm-salesforce", "marketing-asset-drive", "external-cms", "lead-gen-api"],
        "auth": ["password_mfa", "password", "sso_token"],
        "devices": ["Windows 11 Laptop", "Android Mobile", "macOS Air"],
        "ip_pool": ["10.60.2.1", "10.60.2.2", "10.60.2.3"],
        "commands": [
            ["login", "query_crm", "export_leads", "logout"],
            ["login", "upload_asset", "launch_campaign", "logout"],
            ["login", "update_contact", "logout"]
        ]
    },
    "General Admin": {
        "resources": ["intranet-portal", "ticket-system", "shared-public-drive"],
        "auth": ["password", "password_mfa"],
        "devices": ["Windows 10 Enterprise Desktop"],
        "ip_pool": ["10.10.1.1", "10.10.1.2", "10.10.1.3"],
        "commands": [
            ["login", "read_file", "logout"],
            ["login", "view_tickets", "update_ticket", "logout"],
            ["login", "read_intranet", "logout"]
        ]
    },
    "Intern / New Hire": {
        "resources": ["sandbox-env", "training-modules"],
        "auth": ["password", "password_mfa"],
        "devices": ["BYOD Windows 11 Home", "BYOD Ubuntu Desktop", "macOS BYOD"],
        "ip_pool": ["10.10.5.1", "10.10.5.2"],
        "commands": [
            ["login", "watch_tutorial", "logout"],
            ["login", "access_sandbox", "run_test", "logout"],
            ["login", "read_wiki", "logout"]
        ]
    }
}


def build_entity_registry() -> Dict[str, Dict[str, Any]]:
    entities = {}
    user_id_counter = 101

    for role, count in ROLE_COUNTS.items():
        for _ in range(count):
            e_id = f"USR_{user_id_counter}"
            lat = float(fake.latitude())
            lon = float(fake.longitude())
            entities[e_id] = {
                "entity_id": e_id,
                "entity_type": "user",
                "role": role,
                "profile": ROLE_PROFILES[role],
                "base_mac": fake.mac_address(),
                "base_geo": f"{fake.city()}, {fake.country()}",
                "base_lat": lat,
                "base_lon": lon
            }
            user_id_counter += 1

    for i in range(1, NUM_SERVICE_ACCOUNTS + 1):
        e_id = f"SVC_{i:03d}"
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        entities[e_id] = {
            "entity_id": e_id,
            "entity_type": "service_account",
            "role": "service_account",
            "profile": {
                "resources": ["backup-storage", "db-replica-sync"],
                "auth": ["certificate"],
                "devices": ["Linux Server"],
                "ip_pool": [f"10.90.0.{i}"],
                "commands": [
                    ["authenticate", "sync_database", "terminate_session"],
                    ["authenticate", "run_backup", "upload_logs", "terminate_session"]
                ]
            },
            "base_mac": fake.mac_address(),
            "base_geo": "Internal DataCenter",
            "base_lat": lat,
            "base_lon": lon
        }

    for i in range(1, NUM_EDGE_DEVICES + 1):
        e_id = f"EDG_{i:03d}"
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        entities[e_id] = {
            "entity_id": e_id,
            "entity_type": "edge_device",
            "role": "edge_device",
            "profile": {
                "resources": ["sensor-telemetry-endpoint", "ot-gateway-control"],
                "auth": ["certificate"],
                "devices": ["fw_v2.3"],
                "ip_pool": [fake.ipv4_private()],
                "commands": [
                    ["connect", "transmit_telemetry", "disconnect"],
                    ["connect", "check_firmware", "download_update", "disconnect"]
                ]
            },
            "base_mac": fake.mac_address(),
            "base_geo": f"{lat}, {lon}",
            "base_lat": lat,
            "base_lon": lon
        }

    return entities


def get_session_ip(profile: Dict[str, Any]) -> str:
    pool = profile.get("ip_pool", [])
    if not pool:
        return fake.ipv4_private()
    if pool == ["DYNAMIC"]:
        return fake.ipv4_public()
    return random.choice(pool)


def generate_normal_event(entity: Dict[str, Any], timestamp: datetime) -> Dict[str, Any]:
    profile = entity["profile"]
    return {
        "entity_id": entity["entity_id"],
        "entity_type": entity["entity_type"],
        "role": entity["role"],
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": get_session_ip(profile),
        "geo_location": entity["base_geo"],
        "geo_lat": entity["base_lat"],
        "geo_lon": entity["base_lon"],
        "resource_accessed": random.choice(profile["resources"]),
        "auth_method": random.choice(profile["auth"]),
        "auth_result": "success",
        "session_duration_sec": random.randint(30, 3600),
        "command_sequence": random.choice(profile["commands"]),
        "device_fingerprint": random.choice(profile["devices"]),
        "mac_address": entity["base_mac"],
        "label": "normal",
        "deviation_source": "none"
    }


def generate_attack_event(entities: Dict[str, Dict[str, Any]], timestamp: datetime) -> List[Dict[str, Any]]:
    # Weighted attack type sampling (Non-equal probability)
    attack_types = [
        "brute_force",               # 35%
        "credential_stuffing",       # 25%
        "lateral_movement",          # 15%
        "low_and_slow_exfiltration", # 10%
        "device_spoofing",           # 10%
        "impossible_travel"          # 5%
    ]
    weights = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]
    attack_type = random.choices(attack_types, weights=weights, k=1)[0]
    events = []

    if attack_type == "brute_force":
        bf_candidates = [e_id for e_id, info in entities.items() if info["role"] in ["Intern / New Hire", "General Admin"]]
        target_id = random.choice(bf_candidates)
        target = entities[target_id]
        events.append({
            "entity_id": target_id,
            "entity_type": target["entity_type"],
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": fake.ipv4_public(),
            "geo_location": f"{fake.city()}, {fake.country()}",
            "geo_lat": float(fake.latitude()),
            "geo_lon": float(fake.longitude()),
            "resource_accessed": random.choice(target["profile"]["resources"]),
            "auth_method": "password",
            "auth_result": "fail",
            "session_duration_sec": 2,
            "command_sequence": ["login_attempt", "auth_fail"],
            "device_fingerprint": target["profile"]["devices"][0],
            "mac_address": fake.mac_address(),
            "label": "brute_force",
            "deviation_source": "auth_result, timing, source_ip, mac_address"
        })

    elif attack_type == "impossible_travel":
        exec_candidates = [e_id for e_id, info in entities.items() if info["role"] == "Executive"]
        target_id = random.choice(exec_candidates)
        target = entities[target_id]
        events.append({
            "entity_id": target_id,
            "entity_type": "user",
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": fake.ipv4_public(),
            "geo_location": "Tokyo, Japan",
            "geo_lat": 35.6762,
            "geo_lon": 139.6503,
            "resource_accessed": "exec-kpi-dashboard",
            "auth_method": "password_mfa",
            "auth_result": "success",
            "session_duration_sec": 300,
            "command_sequence": ["login", "view_dashboard", "download_summary", "logout"],
            "device_fingerprint": target["profile"]["devices"][0],
            "mac_address": target["base_mac"],
            "label": "impossible_travel",
            "deviation_source": "geo_location, source_ip"
        })

    elif attack_type == "credential_stuffing":
        user_candidates = [e_id for e_id, info in entities.items() if info["entity_type"] == "user"]
        t_id = random.choice(user_candidates)
        target = entities[t_id]
        events.append({
            "entity_id": t_id,
            "entity_type": target["entity_type"],
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": fake.ipv4_public(),
            "geo_location": f"{fake.city()}, {fake.country()}",
            "geo_lat": float(fake.latitude()),
            "geo_lon": float(fake.longitude()),
            "resource_accessed": target["profile"]["resources"][0],
            "auth_method": "password",
            "auth_result": "fail",
            "session_duration_sec": 1,
            "command_sequence": ["login_attempt", "auth_fail"],
            "device_fingerprint": "Automated Python Script / Bot",
            "mac_address": fake.mac_address(),
            "label": "credential_stuffing",
            "deviation_source": "source_ip, mac_address, auth_result"
        })

    elif attack_type == "lateral_movement":
        eng_candidates = [e_id for e_id, info in entities.items() if info["role"] == "Software Engineer"]
        target_id = random.choice(eng_candidates)
        target = entities[target_id]
        events.append({
            "entity_id": target_id,
            "entity_type": "user",
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(target["profile"]),
            "geo_location": target["base_geo"],
            "geo_lat": target["base_lat"],
            "geo_lon": target["base_lon"],
            "resource_accessed": "payroll-db",
            "auth_method": "ssh_key",
            "auth_result": "success",
            "session_duration_sec": 180,
            "command_sequence": ["login", "open_firewall_console", "modify_rules", "logout"],
            "device_fingerprint": target["profile"]["devices"][0],
            "mac_address": target["base_mac"],
            "label": "lateral_movement",
            "deviation_source": "resource_accessed, command_sequence"
        })

    elif attack_type == "device_spoofing":
        edge_candidates = [e_id for e_id, info in entities.items() if info["entity_type"] == "edge_device"]
        target_id = random.choice(edge_candidates)
        target = entities[target_id]
        events.append({
            "entity_id": target_id,
            "entity_type": "edge_device",
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(target["profile"]),
            "geo_location": target["base_geo"],
            "geo_lat": target["base_lat"],
            "geo_lon": target["base_lon"],
            "resource_accessed": "sensor-telemetry-endpoint",
            "auth_method": "certificate",
            "auth_result": "success",
            "session_duration_sec": 45,
            "command_sequence": ["connect", "transmit_telemetry", "disconnect"],
            "device_fingerprint": "Windows 11 Workstation - Mismatched Device",
            "mac_address": fake.mac_address(),
            "label": "device_spoofing",
            "deviation_source": "mac_address, device_fingerprint"
        })

    elif attack_type == "low_and_slow_exfiltration":
        fin_candidates = [e_id for e_id, info in entities.items() if info["role"] == "Finance & Accounting"]
        target_id = random.choice(fin_candidates)
        target = entities[target_id]
        events.append({
            "entity_id": target_id,
            "entity_type": "user",
            "role": target["role"],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(target["profile"]),
            "geo_location": target["base_geo"],
            "geo_lat": target["base_lat"],
            "geo_lon": target["base_lon"],
            "resource_accessed": "payroll-db",
            "auth_method": "hardware_key",
            "auth_result": "success",
            "session_duration_sec": 7200,
            "command_sequence": ["login", "access_erp", "download_report", "export_data", "logout"],
            "device_fingerprint": target["profile"]["devices"][0],
            "mac_address": target["base_mac"],
            "label": "low_and_slow_exfiltration",
            "deviation_source": "timestamp, command_sequence"
        })

    return events


def create_kafka_producer() -> KafkaProducer:
    producer = None
    while producer is None:
        try:
            logger.info(f"Connecting Kafka Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
            logger.info("Successfully connected Kafka Producer!")
        except Exception as e:
            logger.warning(f"Kafka connection failed ({e}). Retrying in 5 seconds...")
            time.sleep(5)
    return producer


def main():
    logger.info("Starting Synthetic Data Generator Microservice (~60 logs/min, 2.5% anomaly rate)...")
    entities = build_entity_registry()
    entity_keys = list(entities.keys())
    producer = create_kafka_producer()

    count = 0
    while True:
        try:
            now = datetime.utcnow()
            if random.random() < ANOMALY_PROBABILITY:
                batch = generate_attack_event(entities, now)
            else:
                target_entity = entities[random.choice(entity_keys)]
                batch = [generate_normal_event(target_entity, now)]

            for event in batch:
                producer.send(TOPIC_RAW_LOGS, value=event)
                count += 1
                if count % 20 == 0:
                    logger.info(f"Streamed {count} logs to topic '{TOPIC_RAW_LOGS}' (Latest: {event['entity_id']}, Label: {event['label']})")

            producer.flush()
            time.sleep(STREAM_INTERVAL_SEC)

        except KeyboardInterrupt:
            logger.info("Generator stopped by user.")
            break
        except Exception as e:
            logger.error(f"Error during log generation: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
