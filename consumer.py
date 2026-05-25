from kafka import KafkaConsumer
import json
import time

while True:
    try:
        consumer = KafkaConsumer(
            'test-topic',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print("Connected to Kafka!")
        break

    except Exception as e:
        print("Kafka not ready yet. Retrying in 5 seconds...")
        time.sleep(5)

print("Waiting for messages...\n")

for message in consumer:
    print(f"Received: {message.value}")