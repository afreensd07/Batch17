import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['CUDA_VISIBLE_DEVICES'] = '-1'
import random
import json
import logging
import joblib
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from logging.handlers import RotatingFileHandler
from inference_model import detect_anomalies
import pandas as pd

# Configuration
KAFKA_ANOMALY_TOPIC = 'flow_anomalies'
KAFKA_TOPIC = 'flow_metrics'
KAFKA_SERVER = 'localhost:9092'
SCALER_PATH = 'rtiot_scaler.pkl'
THRESHOLD_PATH = 'threshold.txt'
FEATURE_ORDER = [
    'duration',
    'fwd_packets',
    'bwd_packets', 
    'fwd_bytes',
    'bwd_bytes',
    'fwd_iat_avg',
    'bwd_iat_avg',
    'packet_rate'
]
BATCH_SIZE = 100

# Configure logging
logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            'anomalies.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
    ],
    level=logging.INFO,
    format='%(message)s'
)

class AnomalyDetector:
    def __init__(self):
        self.scaler = joblib.load(SCALER_PATH)
        self.threshold = self._load_threshold()
        self.anomaly_batch = []
        
        # Validate feature configuration
        assert self.scaler.n_features_in_ == len(FEATURE_ORDER), \
            "Feature mismatch between scaler and configuration!"

        # Initialize Kafka clients
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )   
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def _load_threshold(self):
        with open(THRESHOLD_PATH, 'r') as f:
            return float(f.read().strip())

    def process_message(self, message):
        try:
            flow_data = message.value
            
            if 'metrics' not in flow_data:
                raise ValueError("Invalid message format: missing 'metrics'")

            metrics = flow_data['metrics']
            
            X = pd.DataFrame([[
            metrics.get('duration', 0),
            metrics.get('fwd_packets', 0),
            metrics.get('bwd_packets', 0),
            metrics.get('fwd_bytes', 0),
            metrics.get('bwd_bytes', 0),
            metrics.get('fwd_iat_avg', 0),
            metrics.get('bwd_iat_avg', 0),
            metrics.get('packet_rate', 0)
    ]], columns=FEATURE_ORDER) 
            
            # Normalize features
            X_scaled = self.scaler.transform(X)
            
            # Detect anomaly
            is_anomalous = detect_anomalies(X_scaled,self.threshold)[0]
            flow_data['is_anomaly'] = int(is_anomalous)
            self._send_to_kafka(flow_data)  # Send regardless of anomaly status

            if is_anomalous:
                print(f" Anomaly Detected[{flow_data['flow_id']}]")
                self._log_anomaly(flow_data)
            else:
                print(f"Normal Flow[{flow_data['flow_id']}]")

        except KeyError as e:
            print(f"Missing feature in message: {str(e)}")
        except Exception as e:
            print(f"Error processing message: {str(e)}")

    def _send_to_kafka(self, data):
        try:
            self.anomaly_batch.append(data)
            if len(self.anomaly_batch) >= BATCH_SIZE:
                self.producer.send(
                    KAFKA_ANOMALY_TOPIC,
                    {'batch': self.anomaly_batch}
                ).add_errback(
                    lambda e: print(f"Batch send failed: {e}")
                )
                self.anomaly_batch.clear()
                print('sent')
        except KafkaError as e:
            print(f"Kafka Producer Error: {e}")

    def _log_anomaly(self, flow_data):
        log_entry = {
            'timestamp': flow_data['timestamp'],
            'flow_id': flow_data['flow_id'],
            'metrics': flow_data['metrics']
        }
        logging.info(json.dumps(log_entry))

    def run(self):
        print(f"Starting detector")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self._cleanup()

    def _cleanup(self):
        if self.anomaly_batch:
            self.producer.send(KAFKA_ANOMALY_TOPIC, {'batch': self.anomaly_batch})
            
        self.consumer.close()
        self.producer.flush(timeout=10)
        self.producer.close()
        print("Flushed")

if __name__ == "__main__":
    detector = AnomalyDetector()
    detector.run()