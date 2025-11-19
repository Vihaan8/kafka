import json
from typing import Iterable
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import time

class FlinkStyleProcessor:
    def __init__(self):
        self.windows = {}
        self.window_duration = 60
        self.last_window_flush = time.time()
        
        self.critical_thresholds = {
            "heart_rate": {"min": 40, "max": 140},
            "systolic_bp": {"min": 90, "max": 180},
            "spo2": {"min": 90, "max": 100},
            "temperature": {"min": 35.0, "max": 39.0},
            "respiratory_rate": {"min": 10, "max": 30}
        }
    
    def add_to_window(self, vitals: dict):
        patient_id = vitals["patient_id"]
        
        if patient_id not in self.windows:
            self.windows[patient_id] = {
                "patient_id": patient_id,
                "readings": [],
                "window_start": datetime.now().isoformat()
            }
        
        self.windows[patient_id]["readings"].append(vitals)
    
    def compute_window_statistics(self, readings: list) -> dict:
        if not readings:
            return {}
        
        stats = {
            "count": len(readings),
            "window_start": readings[0]["timestamp"],
            "window_end": readings[-1]["timestamp"]
        }
        
        vitals = ["heart_rate", "systolic_bp", "diastolic_bp", "spo2", "temperature", "respiratory_rate"]
        
        for vital in vitals:
            values = [r[vital] for r in readings]
            stats[f"{vital}_min"] = min(values)
            stats[f"{vital}_max"] = max(values)
            stats[f"{vital}_avg"] = round(sum(values) / len(values), 2)
        
        return stats
    
    def check_critical_alerts(self, stats: dict, patient_id: str) -> list:
        alerts = []
        
        for vital, thresholds in self.critical_thresholds.items():
            max_val = stats.get(f"{vital}_max", 0)
            min_val = stats.get(f"{vital}_min", 0)
            
            if max_val > thresholds["max"]:
                alerts.append({
                    "patient_id": patient_id,
                    "vital_sign": vital,
                    "alert_type": "HIGH",
                    "value": max_val,
                    "threshold": thresholds["max"],
                    "timestamp": datetime.now().isoformat()
                })
            
            if min_val < thresholds["min"]:
                alerts.append({
                    "patient_id": patient_id,
                    "vital_sign": vital,
                    "alert_type": "LOW",
                    "value": min_val,
                    "threshold": thresholds["min"],
                    "timestamp": datetime.now().isoformat()
                })
        
        return alerts
    
    def flush_windows(self, alert_producer: KafkaProducer):
        results = []
        
        for patient_id, window_data in self.windows.items():
            readings = window_data["readings"]
            
            if len(readings) == 0:
                continue
            
            stats = self.compute_window_statistics(readings)
            stats["patient_id"] = patient_id
            
            alerts = self.check_critical_alerts(stats, patient_id)
            
            for alert in alerts:
                alert_producer.send("patient-alerts", value=alert)
                print(f"[Flink] CRITICAL: {alert['patient_id']} {alert['vital_sign']} {alert['alert_type']} "
                      f"Value:{alert['value']} Threshold:{alert['threshold']}")
            
            if len(alerts) > 0:
                print(f"[Flink] Window {patient_id} | Readings:{stats['count']} Alerts:{len(alerts)}")
            else:
                print(f"[Flink] Window {patient_id} | Readings:{stats['count']} "
                      f"HR_avg:{stats.get('heart_rate_avg', 0)} SpO2_avg:{stats.get('spo2_avg', 0)}")
            
            results.append(stats)
        
        self.windows = {}
        self.last_window_flush = time.time()
        
        return results

def run_flink_processor():
    try:
        print("[Flink] Connecting to Kafka at localhost:9092...")
        
        consumer = KafkaConsumer(
            "patient-vitals",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="flink-processor-group",
        )
        
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        
        print("[Flink] Connected. Starting 1-minute windowed processing...\n")
        
        processor = FlinkStyleProcessor()
        
        for message in consumer:
            vitals = message.value
            processor.add_to_window(vitals)
            
            current_time = time.time()
            if current_time - processor.last_window_flush >= processor.window_duration:
                print(f"\n[Flink] Window flush at {datetime.now().strftime('%H:%M:%S')}")
                processor.flush_windows(producer)
                producer.flush()
                print()
        
    except KeyboardInterrupt:
        print("\n[Flink] Shutting down.")
    except Exception as e:
        print(f"[Flink ERROR] {e}")
        raise
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    run_flink_processor()