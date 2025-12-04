import paho.mqtt.client as mqtt
import psycopg2
import json
import datetime
import time
import os
import logging
import sys

# Korrekt import af de funktioner, der er defineret i din logging_config.py
try:
    from .logging_config import configure_logging, create_logging_table
except ImportError:
    # Fallback for direkte eksekvering (hvis ikke kørt som del af en pakke)
    from logging_config import configure_logging, create_logging_table

# --- QuestDB/PostgreSQL Konfiguration ---
QDB_HOST = "localhost"
QDB_PORT = "8812" # QuestDB PostgreSQL wire protocol port

# --- MQTT Konfiguration ---
MQTT_HOST = "localhost"
MQTT_PORT = 8883  # TLS port
MQTT_TOPIC = "sensors/#"
MQTT_USER = "edgeuser"
MQTT_PASS = "Optilogic25"
CA_CERT_PATH = "/home/amir/iot-monitoring/mosquitto/config/certs/ca.crt"

# Global database connection and cursor (styret i __main__)
conn = None
cur = None

# Global logger for dette script
# Dette er den logger, vi vil bruge til at generere logbeskeder (navnet 'mqtt_ingestor' gemmes i loggen)
logger = logging.getLogger('mqtt_ingestor')

# --- QuestDB Database Handlers ---

def create_db_connection(max_retries=10):
    """Etablerer forbindelse til QuestDB ved hjælp af psycopg2."""
    for attempt in range(max_retries):
        try:
            db_conn = psycopg2.connect(
                database="qdb",
                user="admin",
                password="quest",
                host=QDB_HOST,
                port=QDB_PORT
            )
            logger.info("QuestDB dataforbindelse etableret succesfuldt.")
            return db_conn
        except psycopg2.OperationalError as e:
            logger.warning(f"QuestDB dataforbindelse forsøg {attempt+1}/{max_retries} mislykkedes: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                logger.critical("QuestDB dataforbindelse mislykkedes efter maksimum forsøg.", exc_info=True)
                raise

def create_table(cursor):
    """Sikrer, at datatabellen 'olimex_data' eksisterer."""
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
    logger.info("QuestDB datatabel 'olimex_data' kontrolleret/oprettet.")

# --- MQTT Callbacks ---

def on_connect(client, userdata, flags, reason_code, properties=None):
    """Callback for når klienten modtager et CONNACK-svar fra brokeren."""
    if reason_code == 0:
        logger.info(f"MQTT Forbundet succesfuldt til {MQTT_HOST}:{MQTT_PORT}. Abonnerer på {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"MQTT Forbindelse mislykkedes med kode {reason_code}")

def on_message(client, userdata, msg):
    """Callback for når en PUBLISH-besked modtages fra brokeren."""
    global conn, cur

    try:
        # Udpak device_id fra emnet (f.eks. sensors/device_X)
        device_id = msg.topic.split('/')[-1]
        payload = msg.payload.decode()
        data = json.loads(payload)
        
    except Exception as e:
        logger.error(f"Fejl ved parsing af besked fra emne {msg.topic}: {e}", exc_info=True)
        return

    # Rydder data-nøgler, hvis de findes i payloadet
    if "device_id" in data:
        del data["device_id"]
    if "timestamp" in data:
        del data["timestamp"]

    # Bruger aktuel UTC tid for QuestDB
    ts = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    columns = ["ts", "device_id"] + list(data.keys())
    placeholders = ["%s"] * len(columns)

    sql = f"INSERT INTO olimex_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
    params = [ts, device_id] + list(data.values())

    logger.info(f"Modtaget fra emne {msg.topic} for enhed {device_id}. Indsætter i DB.")

    max_retries = 2
    for attempt in range(max_retries):
        try:
            cur.execute(sql, tuple(params))
            conn.commit()
            logger.debug(f"Indsat række for enhed: {device_id}")
            break
        except psycopg2.OperationalError as e:
            logger.warning(f"Database driftsfejl på forsøg {attempt+1}. Genforbindelse nødvendig: {e}")
            if attempt < max_retries - 1:
                logger.info("Forsøger at genoprette forbindelse til QuestDB...")
                try:
                    conn.close()
                    conn = create_db_connection()
                    cur = conn.cursor()
                    # Prøv forespørgslen igen straks i næste løkke
                except Exception as reconnect_e:
                    logger.critical(f"Genforbindelse mislykkedes: {reconnect_e}")
                    time.sleep(1)
            else:
                logger.error("Kunne ikke genoprette forbindelse efter max forsøg. Dropper besked.", exc_info=True)
                break
        except Exception as e:
            logger.critical(f"Fatale fejl under indsættelse: {e}. Dropper besked.", exc_info=True)
            break

if __name__ == '__main__':
    # 1. QuestDB Log Initialisering
    QDB_LOG_CONFIG = {
        'database': 'qdb',
        'user': 'admin',
        'password': 'quest',
        'host': QDB_HOST,
        'port': QDB_PORT
    }
    
    try:
        # Opret 'logging' tabellen ved hjælp af din funktion
        create_logging_table(QDB_LOG_CONFIG)
        
        # Konfigurer root-loggeren. Din funktion sætter niveauet til INFO.
        configure_logging(QDB_LOG_CONFIG)
        
        # Sæt loggerniveauet for at være sikker, da vi henter den her
        logger.setLevel(logging.INFO) 
        
        logger.info("Log-handler succesfuldt konfigureret for QuestDB.")
    except Exception as e:
        # Fallback hvis logningsopsætning mislykkes
        logging.basicConfig(level=logging.INFO)
        # Hent loggeren igen efter basicConfig for at være sikker
        logger = logging.getLogger('mqtt_ingestor')
        logger.error(f"KRITISK: QuestDB logningsopsætning mislykkedes. Bruger kun konsollogning: {e}", exc_info=True)

    # 2. QuestDB Dataforbindelsesopsætning
    try:
        conn = create_db_connection()
        cur = conn.cursor()
        create_table(cur)
    except Exception as e:
        logger.critical(f"Kritisk QuestDB initialiseringsfejl: {e}")
        sys.exit(1)

    # 3. MQTT Klientopsætning
    client = mqtt.Client(client_id="QuestDB_Ingestor", protocol=mqtt.MQTTv5)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    
    # Tjek om CA-certifikatfilen eksisterer
    if os.path.exists(CA_CERT_PATH):
        try:
            client.tls_set(ca_certs=CA_CERT_PATH)
            logger.info(f"TLS aktiveret ved hjælp af CA certifikat: {CA_CERT_PATH}")
        except Exception as e:
            logger.error(f"Kunne ikke sætte TLS konfiguration: {e}")
    else:
        logger.warning(f"CA-certifikat ikke fundet på {CA_CERT_PATH}. Forsøger ukrypteret forbindelse. Hvis broker kræver TLS, vil dette fejle.")

    client.on_connect = on_connect
    client.on_message = on_message

    # 4. Kørslen
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("\nIngestor stoppet af bruger.")
    except Exception as e:
        logger.critical(f"En uventet MQTT-fejl opstod: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("QuestDB dataforbindelse lukket.")
