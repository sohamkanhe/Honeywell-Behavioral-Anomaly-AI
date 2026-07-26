import json
import random
import csv
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Data Generation Configuration
NUM_DAYS = 150  # Scaled up to 5 months to naturally reach ~200k rows
START_DATE = datetime(2025, 6, 1, 8, 0, 0)
OUTPUT_FILE = "ueba_synthetic_dataset_200k.csv"

# Entity Counts (Reconciled to exactly 160 Users + 20 Service + 20 Edge)
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

# Structured Profiles with Array Command Sequences and Shared IP Pools
ROLE_PROFILES = {
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

entities = {}
user_id_counter = 101

def get_session_ip(profile):
    pool = profile.get("ip_pool", [])
    if not pool:
        return fake.ipv4_private()
    if pool == ["DYNAMIC"]:
        return fake.ipv4_public()
    return random.choice(pool)

def get_activity_probability(role, hour):
    # Same normal distributions, just run over 150 days instead of 30
    if role in ["Software Engineer", "Finance & Accounting", "Marketing & Sales", "General Admin", "Intern / New Hire"]:
        weights = {
            8: 0.20, 9: 0.65, 10: 0.85, 11: 0.40, 12: 0.15,
            13: 0.50, 14: 0.70, 15: 0.50, 16: 0.30, 17: 0.10, 18: 0.05
        }
        return weights.get(hour, 0.01)
    elif role in ["Security & IT Admin", "service_account", "edge_device"]:
        return 0.40
    elif role == "Executive":
        return random.choice([0.10, 0.60, 0.90])
    return 0.05

# Build User Profiles
for role, count in ROLE_COUNTS.items():
    for _ in range(count):
        e_id = f"USR_{user_id_counter}"
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        entities[e_id] = {
            "entity_type": "user",
            "role": role,
            "profile": ROLE_PROFILES[role],
            "base_mac": fake.mac_address(),
            "base_geo": f"{fake.city()}, {fake.country()}",
            "base_lat": lat,
            "base_lon": lon
        }
        user_id_counter += 1

# Build Service Accounts
for i in range(1, NUM_SERVICE_ACCOUNTS + 1):
    e_id = f"SVC_{i:03d}"
    lat = float(fake.latitude())
    lon = float(fake.longitude())
    entities[e_id] = {
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

# Build Edge Devices
for i in range(1, NUM_EDGE_DEVICES + 1):
    e_id = f"EDG_{i:03d}"
    lat = float(fake.latitude())
    lon = float(fake.longitude())
    entities[e_id] = {
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

dataset = []

# Generate Benign Background Logs (~195,000 expected over 150 days)
print(f"Generating standard background logs over {NUM_DAYS} days. This may take a minute...")
current_time = START_DATE
for day in range(NUM_DAYS):
    for hour in range(24):
        for e_id, info in entities.items():
            profile = info["profile"]
            if random.random() < get_activity_probability(info["role"], hour):
                timestamp = current_time + timedelta(hours=hour, minutes=random.randint(0, 59), seconds=random.randint(0, 59))
                dataset.append({
                    "entity_id": e_id,
                    "entity_type": info["entity_type"],
                    "role": info["role"],
                    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "source_ip": get_session_ip(profile),
                    "geo_location": info["base_geo"],
                    "geo_lat": info["base_lat"],
                    "geo_lon": info["base_lon"],
                    "resource_accessed": random.choice(profile["resources"]),
                    "auth_method": random.choice(profile["auth"]),
                    "auth_result": "success",
                    "session_duration_sec": random.randint(30, 3600),
                    "command_sequence": json.dumps(random.choice(profile["commands"])),
                    "device_fingerprint": random.choice(profile["devices"]),
                    "mac_address": info["base_mac"],
                    "label": "normal",
                    "deviation_source": "none"
                })
    current_time += timedelta(days=1)


# --- ANOMALY INJECTION PHASE (SCALED FOR ~200K TOTAL ROWS) ---
# Total Anomaly Target: Exactly 5,000 Rows

print("Injecting exactly 5,000 anomaly events...")

# 1. Brute Force (20 attacks * 50 attempts = 1,000 rows)
bf_candidates = [e for e, info in entities.items() if info["role"] in ["Intern / New Hire", "General Admin"]]
for _ in range(20):
    target_bf = random.choice(bf_candidates)
    bf_time = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-2), hours=random.randint(0, 23))
    attacker_ip = fake.ipv4_public()
    attacker_mac = fake.mac_address()

    for i in range(50):
        is_last = (i == 49)
        cmd_seq = ["login_attempt", "auth_success", "read_file", "logout"] if is_last else ["login_attempt", "auth_fail"]
        dataset.append({
            "entity_id": target_bf,
            "entity_type": entities[target_bf]["entity_type"],
            "role": entities[target_bf]["role"],
            "timestamp": (bf_time + timedelta(seconds=i*2)).strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": attacker_ip,
            "geo_location": f"{fake.city()}, {fake.country()}",
            "geo_lat": float(fake.latitude()),
            "geo_lon": float(fake.longitude()),
            "resource_accessed": random.choice(entities[target_bf]["profile"]["resources"]),
            "auth_method": "password",
            "auth_result": "success" if is_last else "fail",
            "session_duration_sec": 2,
            "command_sequence": json.dumps(cmd_seq),
            "device_fingerprint": entities[target_bf]["profile"]["devices"][0],
            "mac_address": attacker_mac,
            "label": "brute_force",
            "deviation_source": "auth_result, timing, source_ip, mac_address"
        })

# 2. Impossible Travel (200 events = 200 rows)
exec_candidates = [e for e, info in entities.items() if info["role"] == "Executive"]
for _ in range(200):
    target_exec = random.choice(exec_candidates)
    travel_time = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-2), hours=random.randint(6, 20))

    # Precursor Normal event
    dataset.append({
        "entity_id": target_exec,
        "entity_type": "user",
        "role": entities[target_exec]["role"],
        "timestamp": travel_time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": get_session_ip(entities[target_exec]["profile"]),
        "geo_location": entities[target_exec]["base_geo"],
        "geo_lat": entities[target_exec]["base_lat"],
        "geo_lon": entities[target_exec]["base_lon"],
        "resource_accessed": "exec-kpi-dashboard",
        "auth_method": "password_mfa",
        "auth_result": "success",
        "session_duration_sec": 600,
        "command_sequence": json.dumps(["login", "view_dashboard", "download_summary", "logout"]),
        "device_fingerprint": entities[target_exec]["profile"]["devices"][0],
        "mac_address": entities[target_exec]["base_mac"],
        "label": "normal",
        "deviation_source": "none"
    })
    # Impossible travel event
    dataset.append({
        "entity_id": target_exec,
        "entity_type": "user",
        "role": entities[target_exec]["role"],
        "timestamp": (travel_time + timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": fake.ipv4_public(),
        "geo_location": "Tokyo, Japan",
        "geo_lat": 35.6762,
        "geo_lon": 139.6503,
        "resource_accessed": "exec-kpi-dashboard",
        "auth_method": "password_mfa",
        "auth_result": "success",
        "session_duration_sec": 300,
        "command_sequence": json.dumps(["login", "view_dashboard", "download_summary", "logout"]),
        "device_fingerprint": entities[target_exec]["profile"]["devices"][0],
        "mac_address": entities[target_exec]["base_mac"],
        "label": "impossible_travel",
        "deviation_source": "geo_location, source_ip"
    })

# 3. Credential Stuffing (40 waves * 25 users = 1,000 rows)
user_candidates = [e for e, info in entities.items() if info["entity_type"] == "user"]
for _ in range(40):
    cs_candidates = random.sample(user_candidates, 25)
    cs_time = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-2), hours=random.randint(1, 4))
    cs_ip = fake.ipv4_public()
    cs_mac = fake.mac_address()

    for idx, e_id in enumerate(cs_candidates):
        dataset.append({
            "entity_id": e_id,
            "entity_type": entities[e_id]["entity_type"],
            "role": entities[e_id]["role"],
            "timestamp": (cs_time + timedelta(seconds=idx*3)).strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": cs_ip,
            "geo_location": f"{fake.city()}, {fake.country()}",
            "geo_lat": float(fake.latitude()),
            "geo_lon": float(fake.longitude()),
            "resource_accessed": entities[e_id]["profile"]["resources"][0],
            "auth_method": "password",
            "auth_result": "fail" if random.random() < 0.92 else "success",
            "session_duration_sec": 1,
            "command_sequence": json.dumps(["login_attempt", "auth_fail"]),
            "device_fingerprint": "Automated Python Script / Bot",
            "mac_address": cs_mac,
            "label": "credential_stuffing",
            "deviation_source": "source_ip, mac_address, auth_result"
        })

# 4. Lateral Movement (100 attacks * 10 steps = 1,000 rows)
eng_candidates = [e for e, info in entities.items() if info["role"] == "Software Engineer"]
unauthorized_resources = ["payroll-db", "erp-corporate", "firewall-config-console", "board-strategy-docs", "auth-server-logs"]
for _ in range(100):
    target_eng = random.choice(eng_candidates)
    lm_time = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-2), hours=random.randint(18, 23))

    for idx, res in enumerate(unauthorized_resources * 2):
        dataset.append({
            "entity_id": target_eng,
            "entity_type": "user",
            "role": entities[target_eng]["role"],
            "timestamp": (lm_time + timedelta(minutes=idx*2)).strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(entities[target_eng]["profile"]),
            "geo_location": entities[target_eng]["base_geo"],
            "geo_lat": entities[target_eng]["base_lat"],
            "geo_lon": entities[target_eng]["base_lon"],
            "resource_accessed": res,
            "auth_method": "ssh_key",
            "auth_result": "success",
            "session_duration_sec": 180,
            "command_sequence": json.dumps(["login", "open_firewall_console", "modify_rules", "export_logs", "logout"]),
            "device_fingerprint": entities[target_eng]["profile"]["devices"][0],
            "mac_address": entities[target_eng]["base_mac"],
            "label": "lateral_movement",
            "deviation_source": "resource_accessed, command_sequence"
        })

# 5. Device Spoofing (300 events = 300 rows)
edge_candidates = [e for e, info in entities.items() if info["entity_type"] == "edge_device"]
for _ in range(300):
    edge_target = random.choice(edge_candidates)
    ds_time = START_DATE + timedelta(days=random.randint(1, NUM_DAYS-2), hours=random.randint(0, 23))
    dataset.append({
        "entity_id": edge_target,
        "entity_type": "edge_device",
        "role": entities[edge_target]["role"],
        "timestamp": ds_time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": get_session_ip(entities[edge_target]["profile"]),
        "geo_location": entities[edge_target]["base_geo"],
        "geo_lat": entities[edge_target]["base_lat"],
        "geo_lon": entities[edge_target]["base_lon"],
        "resource_accessed": "sensor-telemetry-endpoint",
        "auth_method": "certificate",
        "auth_result": "success",
        "session_duration_sec": 45,
        "command_sequence": json.dumps(["connect", "transmit_telemetry", "disconnect"]),
        "device_fingerprint": "Windows 11 Workstation - Mismatched Device",
        "mac_address": fake.mac_address(),
        "label": "device_spoofing",
        "deviation_source": "mac_address, device_fingerprint"
    })

# 6. Low-and-Slow Exfiltration (40 users * 15 extractions = 600 rows)
fin_candidates = [e for e, info in entities.items() if info["role"] == "Finance & Accounting"]
# Use combinations if short on users, or just oversample
for target_fin in random.choices(fin_candidates, k=40):
    # Select 15 random days for this user to exfil
    exfil_days = sorted(random.sample(range(1, NUM_DAYS-2), 15))
    for day_offset in exfil_days:
        exfil_time = START_DATE + timedelta(days=day_offset, hours=2, minutes=random.randint(10, 45))
        dataset.append({
            "entity_id": target_fin,
            "entity_type": "user",
            "role": entities[target_fin]["role"],
            "timestamp": exfil_time.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(entities[target_fin]["profile"]),
            "geo_location": entities[target_fin]["base_geo"],
            "geo_lat": entities[target_fin]["base_lat"],
            "geo_lon": entities[target_fin]["base_lon"],
            "resource_accessed": "payroll-db",
            "auth_method": "hardware_key",
            "auth_result": "success",
            "session_duration_sec": 2400,
            "command_sequence": json.dumps(["login", "access_erp", "download_report", "export_data", "logout"]),
            "device_fingerprint": entities[target_fin]["profile"]["devices"][0],
            "mac_address": entities[target_fin]["base_mac"],
            "label": "low_and_slow_exfiltration",
            "deviation_source": "timestamp, command_sequence"
        })

# 7. Insider Drift (50 users * 18 events = 900 rows)
for drift_eng in random.choices(eng_candidates, k=50):
    # Select 18 random days across the 150 day window
    drift_days = sorted(random.sample(range(1, NUM_DAYS-2), 18))
    for i, day_offset in enumerate(drift_days):
        drift_time = START_DATE + timedelta(days=day_offset, hours=random.randint(9, 17))
        # Escalate drift severity over time
        is_advanced_drift = i > 9
        dataset.append({
            "entity_id": drift_eng,
            "entity_type": "user",
            "role": entities[drift_eng]["role"],
            "timestamp": drift_time.strftime("%Y-%m-%d %H:%M:%S"),
            "source_ip": get_session_ip(entities[drift_eng]["profile"]),
            "geo_location": entities[drift_eng]["base_geo"],
            "geo_lat": entities[drift_eng]["base_lat"],
            "geo_lon": entities[drift_eng]["base_lon"],
            "resource_accessed": "firewall-config-console" if is_advanced_drift else "git-repo-backend",
            "auth_method": "ssh_key",
            "auth_result": "success",
            "session_duration_sec": 600,
            "command_sequence": json.dumps(["login", "clone_repo", "view_iam_policies", "modify_iam_roles", "logout"] if is_advanced_drift else ["login", "clone_repo", "commit_code", "push_code", "logout"]),
            "device_fingerprint": entities[drift_eng]["profile"]["devices"][0],
            "mac_address": entities[drift_eng]["base_mac"],
            "label": "insider_drift",
            "deviation_source": "resource_accessed, command_sequence"
        })

# Sort Final Dataset chronologically
dataset.sort(key=lambda x: x["timestamp"])

fieldnames = [
    "entity_id", "entity_type", "role", "timestamp", "source_ip", "geo_location",
    "geo_lat", "geo_lon", "resource_accessed", "auth_method", "auth_result",
    "session_duration_sec", "command_sequence", "device_fingerprint", "mac_address",
    "label", "deviation_source"
]

with open(OUTPUT_FILE, mode="w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(dataset)

print(f"Successfully generated {len(dataset)} records into {OUTPUT_FILE}")