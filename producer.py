from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers = 'kafka:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)

topic = 'test-topic'

count = 0

while True:
    data = {
        "id": count,
        "message": f"Hello Kafka {count}"
    }

    producer.send(topic, value=data)
    print(f"Producer: {data}")
    count += 1
    time.sleep(1)
