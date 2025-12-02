#!/usr/bin/env python3
import json
import time
import random
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# =======================
# CONFIG
# =======================

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/olimex"  # mqtt_ingestor listens on sensors/#


def generate_mock_payload():
    """
    Generate one random OLIMEX-like measurement matching olimex_data schema.
    """
    now = datetime.now(timezone.utc).isoformat()

    payload = {
        # timestamp
        "ts": now,

        # efficiency & mode
        "heat_exchanger_efficiency": round(random.uniform(0.60, 0.95), 3),
        "run_mode": random.randint(0, 3),

        # temperatures (°C)
        "outdoor_temp": round(random.uniform(-5.0, 15.0), 1),
        "supply_air_temp": round(random.uniform(16.0, 24.0), 1),
        "supply_air_setpoint_temp": 21.0,
        "exhaust_air_temp": round(random.uniform(18.0, 26.0), 1),
        "extract_air_temp": round(random.uniform(18.0, 24.0), 1),

        # pressures (Pa)
        "supply_air_pressure": round(random.uniform(80.0, 200.0), 1),
        "extract_air_pressure": round(random.uniform(80.0, 200.0), 1),

        # flows (m3/h)
        "supply_air_flow": round(random.uniform(100.0, 400.0), 1),
        "extract_air_flow": round(random.uniform(100.0, 400.0), 1),
        "extra_supply_air_flow": round(random.uniform(0.0, 50.0), 1),
        "extra_extract_air_flow": round(random.uniform(0.0, 50.0), 1),

        # runtimes (seconds)
        "supply_air_fan_runtime": random.randint(1_000, 100_000),
        "extract_air_fan_runtime": random.randint(1_000, 100_000),
    }

    return payload


def main():
    client = mqtt.Client()
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    print(f"[simulate_sensors] Publishing to MQTT '{MQTT_TOPIC}' every 5 seconds...")
    try:
        while True:
            payload = generate_mock_payload()
            client.publish(MQTT_TOPIC, json.dumps(payload))
            print("[simulate_sensors] Published:", payload)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n[simulate_sensors] Stopping publisher...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
