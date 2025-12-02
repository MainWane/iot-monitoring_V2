import paho.mqtt.client as mqtt
import psycopg2
import json
import datetime
import time
import os

# --- Configuration ---
QDB_HOST = "localhost"
QDB_PORT = "8812"

MQTT_HOST = "localhost"
MQTT_PORT = 8883  # TLS port
MQTT_TOPIC = "sensors/#"
MQTT_USER = "edgeuser"
MQTT_PASS = "Optilogic25"

CA_CERT_PATH = "/home/amir/iot-monitoring/mosquitto/config/certs/ca.crt"

conn = None
cur = None

def create_db_connection(max_retries=10):
    for attempt in range(max_retries):
        try:
            db_conn = psycopg2.connect(
                database="qdb",
                user="admin",
                password="quest",
                host=QDB_HOST,
                port=QDB_PORT
            )
            print("✓ QuestDB connection established successfully.")
            return db_conn
        except psycopg2.OperationalError as e:
            print(f"QuestDB connection attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise

def create_table(cursor):
    table_create_sql = """
    CREATE TABLE IF NOT EXISTS olimex_data (
        ts TIMESTAMP,
        device_id SYMBOL,
        heat_exchanger_efficiency DOUBLE,
        run_mode INT,
        outdoor_temp DOUBLE,
        supply_air_temp DOUBLE,
        supply_air_setpoint_temp DOUBLE,
        exhaust_air_temp DOUBLE,
        extract_air_temp DOUBLE,
        supply_air_pressure DOUBLE,
        extract_air_pressure DOUBLE,
        supply_air_flow DOUBLE,
        extract_air_flow DOUBLE,
        extra_supply_air_flow DOUBLE,
        extra_extract_air_flow DOUBLE,
        supply_air_fan_runtime LONG,
        extract_air_fan_runtime LONG
    ) TIMESTAMP(ts)
    PARTITION BY DAY;
    """
    cursor.execute(table_create_sql)
    conn.commit()
    print("✓ QuestDB table 'olimex_data' checked/created.")

def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"✓ MQTT Connected successfully to {MQTT_HOST}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"✗ MQTT Connection failed with code {reason_code}")

def on_message(client, userdata, msg):
    global conn, cur

    try:
        device_id = msg.topic.split('/')[-1]
        payload = msg.payload.decode()
        data = json.loads(payload)
    except Exception as e:
        print(f"Error parsing message: {e}")
        return

    if "device_id" in data:
        del data["device_id"]
    if "timestamp" in data:
        del data["timestamp"]

    ts = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    columns = ["ts", "device_id"] + list(data.keys())
    placeholders = ["%s"] * len(columns)

    sql = f"INSERT INTO olimex_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    params = [ts, device_id] + list(data.values())

    print(f"[{ts.isoformat()}] Received from topic {msg.topic} for device {device_id}: {data}")

    max_retries = 2
    for attempt in range(max_retries):
        try:
            cur.execute(sql, tuple(params))
            conn.commit()
            print(f"Inserted row for device: {device_id}")
            break
        except psycopg2.OperationalError as e:
            print(f"Database Operational Error on attempt {attempt+1}: {e}")
            if attempt < max_retries - 1:
                print("Attempting to reconnect to QuestDB...")
                try:
                    conn.close()
                    conn = create_db_connection()
                    cur = conn.cursor()
                except Exception as reconnect_e:
                    print(f"Reconnection failed: {reconnect_e}")
                time.sleep(1)
            else:
                print("✗ Failed to reconnect after max retries. Dropping message.")
        except Exception as e:
            print(f"✗ Fatal error during insertion: {e}. Dropping message.")
            break

if __name__ == '__main__':
    try:
        conn = create_db_connection()
        cur = conn.cursor()
        create_table(cur)
    except Exception as e:
        print(f"Critical QuestDB initialization error: {e}")
        exit(1)

    # Create MQTT client with latest callback API for MQTT v5
    client = mqtt.Client(client_id="QuestDB_Ingestor", protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set(ca_certs=CA_CERT_PATH)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nIngestor stopped by user.")
    except Exception as e:
        print(f"An unexpected MQTT error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("QuestDB connection closed.")
