import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

def run_consumer():
    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "patient-vitals",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="vitals-consumer-group",
        )
        print("[Consumer] Connected to Kafka.")
        
        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] Connected to PostgreSQL.")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS patient_vitals (
                id SERIAL PRIMARY KEY,
                patient_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                heart_rate INTEGER,
                systolic_bp INTEGER,
                diastolic_bp INTEGER,
                spo2 INTEGER,
                temperature NUMERIC(4, 1),
                respiratory_rate INTEGER,
                patient_age INTEGER,
                gender VARCHAR(10),
                condition VARCHAR(50),
                is_anomaly BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_patient_timestamp ON patient_vitals(patient_id, timestamp DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON patient_vitals(timestamp DESC);")
        
        print("[Consumer] Table ready. Listening...\n")

        message_count = 0
        for message in consumer:
            try:
                vitals = message.value
                
                insert_query = """
                    INSERT INTO patient_vitals (
                        patient_id, timestamp, heart_rate, systolic_bp, 
                        diastolic_bp, spo2, temperature, respiratory_rate,
                        patient_age, gender, condition, is_anomaly
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cur.execute(
                    insert_query,
                    (
                        vitals["patient_id"],
                        vitals["timestamp"],
                        vitals["heart_rate"],
                        vitals["systolic_bp"],
                        vitals["diastolic_bp"],
                        vitals["spo2"],
                        vitals["temperature"],
                        vitals["respiratory_rate"],
                        vitals["patient_age"],
                        vitals["gender"],
                        vitals["condition"],
                        vitals.get("is_anomaly", False),
                    ),
                )
                message_count += 1
                
                status = "ALERT" if vitals.get("is_anomaly") else "OK"
                
                print(f"[Consumer] {message_count} Stored {vitals['patient_id']} | "
                      f"HR:{vitals['heart_rate']} BP:{vitals['systolic_bp']}/{vitals['diastolic_bp']} "
                      f"SpO2:{vitals['spo2']} | {status}")
                
            except Exception as e:
                print(f"[Consumer ERROR] {e}")
                continue
                
    except KeyboardInterrupt:
        print("\n[Consumer] Shutting down.")
    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        raise
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    run_consumer()