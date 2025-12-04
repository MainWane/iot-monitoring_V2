import paho.mqtt.client as mqtt
import json
import time
import random
import os
import sys

# --- MQTT Konfiguration (SKAL MATCHES MED MOSQUITTO & INGESTOR) ---
MQTT_HOST = "localhost"
MQTT_PORT = 8883  # Sikker port, der kræver TLS
MQTT_USER = "edgeuser"
MQTT_PASS = "Optilogic25"
# VIGTIGT: Tjek at denne sti er 100% korrekt for dit system!
CA_CERT_PATH = "/home/amir/iot-monitoring/mosquitto/config/certs/ca.crt" 
DEVICES = ["device_1", "device_2", "device_3"]
# Simulation bruger de enkelte device topics: sensors/device_X

def generate_sensor_data(device_id):
    """Genererer realistiske tilfældige sensordata for en enhed."""
    
    # Juster basisværdier baseret på enhed (for at simulere variation)
    if device_id == "device_1":
        base_temp = 22.0
        base_efficiency = 0.9
    elif device_id == "device_2":
        base_temp = 20.0
        base_efficiency = 0.75
    else:
        base_temp = 24.0
        base_efficiency = 0.82

    data = {
        # Kerneværdier
        "heat_exchanger_efficiency": round(random.uniform(base_efficiency - 0.05, base_efficiency + 0.05), 2),
        "run_mode": 1,
        "outdoor_temp": round(random.uniform(5.0, 10.0), 1),
        
        # Luftstrømstemperaturer
        "supply_air_temp": round(random.uniform(base_temp - 0.5, base_temp + 0.5), 1),
        "supply_air_setpoint_temp": base_temp,
        "exhaust_air_temp": round(random.uniform(18.0, 20.0), 1),
        "extract_air_temp": round(random.uniform(20.0, 22.0), 1),
        
        # Tryk og Flow
        "supply_air_pressure": round(random.uniform(100.0, 105.0), 1),
        "extract_air_pressure": round(random.uniform(95.0, 100.0), 1),
        "supply_air_flow": round(random.uniform(120.0, 130.0), 1),
        "extract_air_flow": round(random.uniform(115.0, 125.0), 1),
        
        # Runtime
        "supply_air_fan_runtime": random.randint(86400, 90000),
        "extract_air_fan_runtime": random.randint(86400, 90000),
    }
    return data

def run_simulation():
    """Hovedløkke til at oprette forbindelse og udgive data."""
    # Sætter en unik klient ID og bruger MQTTv5 protokollen
    client = mqtt.Client(client_id="Sensor_Simulator", protocol=mqtt.MQTTv5)
    # Autentificerer med brugernavn og password
    client.username_pw_set(MQTT_USER, MQTT_PASS)

    # VIGTIGT: Indstil TLS og CA-certifikat
    if os.path.exists(CA_CERT_PATH):
        try:
            client.tls_set(ca_certs=CA_CERT_PATH)
            print(f"✅ TLS aktiveret. Forsøger at oprette forbindelse til {MQTT_HOST}:{MQTT_PORT} (Sikker port)")
        except Exception as e:
            print(f"❌ Kunne ikke sætte TLS konfiguration: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"❌ KRITISK: CA-certifikat ikke fundet på {CA_CERT_PATH}. Broker vil afvise forbindelsen.", file=sys.stderr)
        sys.exit(1)

    try:
        # Opretter forbindelse til den sikre port 8883
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()  # Starter baggrundstråd til at håndtere netværkstrafik
    except Exception as e:
        print(f"❌ Forbindelse til MQTT-broker mislykkedes (tjek Mosquitto status): {e}", file=sys.stderr)
        sys.exit(1)

    print(f"🚀 Simulation startet. Sender data for {len(DEVICES)} enheder hvert 5. sekund...")

    try:
        while True:
            for device_id in DEVICES:
                topic = f"sensors/{device_id}" 
                payload = generate_sensor_data(device_id)
                
                # Udgiv til broker med QoS 1
                result = client.publish(topic, json.dumps(payload), qos=1)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"[{time.strftime('%H:%M:%S')}] ✅ PUBLISH OK | Enhed: {device_id} | Temp: {payload['outdoor_temp']} °C")
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] ❌ PUBLISH FEJL | Enhed: {device_id} | Returkode: {result.rc}", file=sys.stderr)
            
            time.sleep(5) 

    except KeyboardInterrupt:
        print("\nSimulation stoppet af bruger.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("MQTT-klient afbrudt.")

if __name__ == '__main__':
    run_simulation()
