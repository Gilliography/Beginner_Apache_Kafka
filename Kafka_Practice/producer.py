from confluent_kafka import Producer
import json

producer_config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered: {msg.value().decode('utf-8')} to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

order = {
    "order_id": str(12345),
    "user_name": "joe",
    "item": "laptop",
    "quantity": 1
}

value= json.dumps(order).encode('utf-8')

producer.produce(
    topic='orders',
    value=value,
    callback=delivery_report()
)

producer.flush()