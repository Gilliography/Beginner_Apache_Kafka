import json
import time
from confluent_kafka import Producer

# Configuration
config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(config)

# Delivery callback to verify message status
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

topic = 'orders'

# Simulate sending 10 messages
for i in range(1, 11):
    order = {
        "order_id": f"ORD-{1000 + i}",
        "item": "laptop" if i % 2 == 0 else "mouse",
        "quantity": i
    }
    
    payload = json.dumps(order).encode('utf-8')
    
    # Optional: pass a key (e.g., order_id) to ensure messages with the same key 
    # go to the same partition
    key = str(order["order_id"]).encode('utf-8')

    producer.produce(
        topic=topic, 
        key=key, 
        value=payload, 
        callback=delivery_report
    )
    
    # Serve delivery callbacks from previous produce calls
    producer.poll(0)
    time.sleep(0.5)

# Wait for all messages to finish transmitting
producer.flush()
