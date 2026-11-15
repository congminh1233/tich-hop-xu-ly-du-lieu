import hashlib
import ipaddress
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    tmp.replace(path)


def now():
    return datetime.now(timezone.utc).isoformat()


def canonical_ip(value):
    try:
        return str(ipaddress.ip_address(str(value).strip()))
    except ValueError:
        return None


def source_id(filename):
    name = Path(filename).name.lower()
    if re.fullmatch(r"network_dataset_\d+\.csv", name):
        return "network"
    if name in ("windows7_dataset.csv", "windows10_dataset.csv"):
        return name.split("_")[0]
    sensors = {"weather", "modbus", "garage_door", "fridge", "gps_tracker",
               "motion_light", "thermostat"}
    if name.startswith("iot_") and name.endswith(".csv") and name[4:-4] in sensors:
        return "iot_" + name[4:-4]
    raise ValueError(f"Unsupported processed file: {filename}. Train_Test is not a timeline source.")


SENSORS = {
    "iot_weather": {"temperature": "number", "pressure": "number", "humidity": "number"},
    "iot_modbus": {"FC1_Read_Input_Register": "number", "FC2_Read_Discrete_Value": "number",
                   "FC3_Read_Holding_Register": "number", "FC4_Read_Coil": "number"},
    "iot_garage_door": {"door_state": "category", "sphone_signal": "boolean"},
    "iot_fridge": {"fridge_temperature": "number", "temp_condition": "category"},
    "iot_gps_tracker": {"latitude": "number", "longitude": "number"},
    "iot_motion_light": {"motion_status": "boolean", "light_status": "category"},
    "iot_thermostat": {"current_temperature": "number", "thermostat_status": "boolean"},
}

NETWORK_REQUIRED = {"ts", "src_ip", "dst_ip", "src_port", "dst_port", "proto", "duration",
                    "src_bytes", "dst_bytes", "src_pkts", "dst_pkts", "conn_state", "label", "type"}


def norm_name(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


HOST_ALIASES = {
    "host_cpu_pct": {"processor total pct processor time", "processor pct processor time"},
    "memory_available_bytes": {"memory available bytes"},
    "memory_committed_pct": {"memory pct committed bytes in use"},
}


def host_columns(columns):
    result = {}
    for metric, aliases in HOST_ALIASES.items():
        allowed = {norm_name(x) for x in aliases}
        found = [c for c in columns if norm_name(c) in allowed]
        if len(found) > 1:
            raise ValueError(f"Ambiguous reviewed mapping {metric}: {found}")
        if found:
            result[metric] = found[0]
    if "host_cpu_pct" not in result:
        raise ValueError("Missing reviewed host CPU field; process CPU cannot replace it.")
    return result


def validate_header(source, columns):
    if len(columns) != len(set(columns)) or any(c.startswith("_it_") for c in columns):
        raise ValueError("Duplicate/reserved source column name")
    required = NETWORK_REQUIRED if source == "network" else {"ts", "label", "type"}
    if source in SENSORS:
        required = {"date", "time", "label", "type"} | set(SENSORS[source])
    missing = required - set(columns)
    if missing:
        raise ValueError(f"{source}: missing required columns {sorted(missing)}")
    if source.startswith("windows"):
        host_columns(columns)
