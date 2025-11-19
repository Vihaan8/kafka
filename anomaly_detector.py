import json
import time
import psycopg2
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
from kafka import KafkaConsumer

class VitalSignsAnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(
            contamination=0.1,
            random_state=42,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_names = [
            "heart_rate", "systolic_bp", "diastolic_bp", 
            "spo2", "temperature", "respiratory_rate"
        ]
        
        self.z_score_threshold = 3.0
        self.historical_stats = {}
    
    def fetch_training_data(self, conn) -> np.ndarray:
        try:
            cur = conn.cursor()
            
            query = """
                SELECT heart_rate, systolic_bp, diastolic_bp, 
                       spo2, temperature, respiratory_rate
                FROM patient_vitals
                WHERE is_anomaly = FALSE
                ORDER BY timestamp DESC
                LIMIT 500;
            """
            
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            
            if len(rows) < 50:
                print(f"[ML] Insufficient training data ({len(rows)} rows). Need at least 50.")
                return None
            
            return np.array(rows)
            
        except Exception as e:
            print(f"[ML ERROR] Failed to fetch training data: {e}")
            return None
    
    def train_model(self, conn):
        print("[ML] Fetching training data...")
        data = self.fetch_training_data(conn)
        
        if data is None or len(data) < 50:
            print("[ML] Cannot train model - using statistical methods only.")
            return False
        
        print(f"[ML] Training Isolation Forest on {len(data)} samples...")
        
        data_scaled = self.scaler.fit_transform(data)
        self.model.fit(data_scaled)
        self.is_trained = True
        
        for i, feature in enumerate(self.feature_names):
            self.historical_stats[feature] = {
                "mean": np.mean(data[:, i]),
                "std": np.std(data[:, i])
            }
        
        print("[ML] Model trained successfully.")
        return True
    
    def extract_features(self, vitals: dict) -> np.ndarray:
        return np.array([
            vitals["heart_rate"],
            vitals["systolic_bp"],
            vitals["diastolic_bp"],
            vitals["spo2"],
            vitals["temperature"],
            vitals["respiratory_rate"]
        ]).reshape(1, -1)
    
    def detect_anomaly_ml(self, vitals: dict) -> dict:
        if not self.is_trained:
            return {"is_anomaly": False, "score": 0.0, "method": "not_trained"}
        
        features = self.extract_features(vitals)
        features_scaled = self.scaler.transform(features)
        
        prediction = self.model.predict(features_scaled)[0]
        anomaly_score = self.model.score_samples(features_scaled)[0]
        
        return {
            "is_anomaly": prediction == -1,
            "score": float(anomaly_score),
            "method": "isolation_forest"
        }
    
    def detect_anomaly_statistical(self, vitals: dict) -> dict:
        if not self.historical_stats:
            return {"is_anomaly": False, "score": 0.0, "method": "no_stats"}
        
        z_scores = []
        anomalous_features = []
        
        for feature in self.feature_names:
            if feature in self.historical_stats:
                value = vitals[feature]
                mean = self.historical_stats[feature]["mean"]
                std = self.historical_stats[feature]["std"]
                
                if std > 0:
                    z_score = abs((value - mean) / std)
                    z_scores.append(z_score)
                    
                    if z_score > self.z_score_threshold:
                        anomalous_features.append({
                            "feature": feature,
                            "value": value,
                            "z_score": round(z_score, 2)
                        })
        
        max_z_score = max(z_scores) if z_scores else 0.0
        
        return {
            "is_anomaly": len(anomalous_features) > 0,
            "score": float(max_z_score),
            "method": "z_score",
            "anomalous_features": anomalous_features
        }
    
    def compute_risk_score(self, vitals: dict, ml_result: dict, stat_result: dict) -> dict:
        risk_score = 0
        risk_factors = []
        
        if ml_result["is_anomaly"]:
            risk_score += 50
            risk_factors.append("ML detection")
        
        if stat_result["is_anomaly"]:
            risk_score += 40
            risk_factors.append("Statistical outlier")
            
        if vitals.get("is_anomaly", False):
            risk_score += 10
            risk_factors.append("Producer flagged")
        
        if vitals["spo2"] < 92:
            risk_score += 30
            risk_factors.append("Low SpO2")
        
        if vitals["heart_rate"] > 130 or vitals["heart_rate"] < 50:
            risk_score += 20
            risk_factors.append("Abnormal HR")
        
        if risk_score >= 80:
            risk_level = "CRITICAL"
        elif risk_score >= 50:
            risk_level = "HIGH"
        elif risk_score >= 30:
            risk_level = "MODERATE"
        else:
            risk_level = "LOW"
        
        return {
            "risk_score": risk_score,
            "risk_level": risk_level,
            "risk_factors": risk_factors,
            "ml_result": ml_result,
            "statistical_result": stat_result
        }

def run_anomaly_detector():
    try:
        print("[ML] Connecting to PostgreSQL for training data...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        
        detector = VitalSignsAnomalyDetector()
        
        print("[ML] Waiting 30 seconds for initial data collection...")
        time.sleep(30)
        
        detector.train_model(conn)
        conn.close()
        
        print("\n[ML] Connecting to Kafka for real-time detection...")
        consumer = KafkaConsumer(
            "patient-vitals",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="anomaly-detector-group",
        )
        
        print("[ML] Connected. Starting real-time anomaly detection...\n")
        
        detection_count = 0
        anomaly_count = 0
        
        for message in consumer:
            vitals = message.value
            
            ml_result = detector.detect_anomaly_ml(vitals)
            stat_result = detector.detect_anomaly_statistical(vitals)
            risk_assessment = detector.compute_risk_score(vitals, ml_result, stat_result)
            
            detection_count += 1
            
            if risk_assessment["risk_level"] in ["HIGH", "CRITICAL"]:
                anomaly_count += 1
                print(f"[ML] {risk_assessment['risk_level']} RISK: {vitals['patient_id']} | "
                      f"Score:{risk_assessment['risk_score']} | "
                      f"Factors:{', '.join(risk_assessment['risk_factors'])}")
            elif detection_count % 20 == 0:
                print(f"[ML] Analyzed {detection_count} readings | "
                      f"Anomalies:{anomaly_count} ({anomaly_count/detection_count*100:.1f}%)")
        
    except KeyboardInterrupt:
        print("\n[ML] Shutting down.")
    except Exception as e:
        print(f"[ML ERROR] {e}")
        raise
    finally:
        consumer.close()

if __name__ == "__main__":
    run_anomaly_detector()