from kafka import KafkaConsumer
import json

# Configuration
TOPIC_NAME = 'flow_metrics'
BOOTSTRAP_SERVERS = ['localhost:9092']  # Update if using remote brokers

# Create consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',  # Start from beginning
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Subscribed to topic: {TOPIC_NAME}")
print("Waiting for messages... (Ctrl+C to stop)\n")

try:
    for message in consumer:
        print(f"Partition: {message.partition}")
        print(f"Offset: {message.offset}")
        print("Value:")
        print(json.dumps(message.value, indent=2))
        print("-" * 50)
        
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()