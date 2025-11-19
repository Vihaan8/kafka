import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

fake = Faker()

PATIENTS = [
    {"patient_id": f"PT-{str(i).zfill(3)}", 
     "age": random.randint(25, 85), 
     "gender": random.choice(["M", "F"]),
     "baseline_condition": random.choice(["Stable", "Moderate", "Critical"])}
    for i in range(1, 21)
]

def generate_vital_signs(patient: dict) -> dict:
    condition = patient["baseline_condition"]
    
    if condition == "Stable":
        hr_range = (60, 90)
        sys_bp_range = (110, 130)
        dia_bp_range = (70, 85)
        spo2_range = (95, 100)
        temp_range = (36.5, 37.2)
        rr_range = (12, 18)
    elif condition == "Moderate":
        hr_range = (85, 110)
        sys_bp_range = (130, 150)
        dia_bp_range = (85, 95)
        spo2_range = (92, 97)
        temp_range = (37.2, 38.0)
        rr_range = (18, 24)
    else:
        hr_range = (110, 140)
        sys_bp_range = (90, 180)
        dia_bp_range = (60, 100)
        spo2_range = (88, 94)
        temp_range = (38.0, 39.5)
        rr_range = (24, 35)
    
    anomaly = random.random() < 0.10
    
    if anomaly:
        heart_rate = random.randint(40, 180)
        systolic_bp = random.randint(70, 200)
        diastolic_bp = random.randint(40, 120)
        spo2 = random.randint(75, 100)
        temperature = round(random.uniform(35.0, 40.0), 1)
        respiratory_rate = random.randint(8, 40)
    else:
        heart_rate = random.randint(*hr_range)
        systolic_bp = random.randint(*sys_bp_range)
        diastolic_bp = random.randint(*dia_bp_range)
        spo2 = random.randint(*spo2_range)
        temperature = round(random.uniform(*temp_range), 1)
        respiratory_rate = random.randint(*rr_range)
    
    return {
        "patient_id": patient["patient_id"],
        "timestamp": datetime.now().isoformat(),
        "heart_rate": heart_rate,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "spo2": spo2,
        "temperature": temperature,
        "respiratory_rate": respiratory_rate,
        "patient_age": patient["age"],
        "gender": patient["gender"],
        "condition": patient["baseline_condition"],
        "is_anomaly": anomaly
    }

def run_producer():
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print(f"[Producer] Connected. Monitoring {len(PATIENTS)} patients.\n")
        
        count = 0
        while True:
            patient = PATIENTS[count % len(PATIENTS)]
            vital_signs = generate_vital_signs(patient)
            
            status = "ALERT" if vital_signs["is_anomaly"] else "OK"
            
            print(f"[Producer] {count} {vital_signs['patient_id']} | "
                  f"HR:{vital_signs['heart_rate']} BP:{vital_signs['systolic_bp']}/{vital_signs['diastolic_bp']} "
                  f"SpO2:{vital_signs['spo2']} Temp:{vital_signs['temperature']} RR:{vital_signs['respiratory_rate']} | {status}")
            
            producer.send("patient-vitals", value=vital_signs)
            producer.flush()
            count += 1
            
            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print("\n[Producer] Shutting down.")
    except Exception as e:
        print(f"[Producer ERROR] {e}")
        raise
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()