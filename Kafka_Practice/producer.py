import json
import time
import requests
from confluent_kafka import Producer

config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(config)
topic = 'api_stream'

# Example public API (CoinDesk Bitcoin Price Index)
API_URL = "https://coindesk.com"

print("🚀 Starting real-time API ingestion loop...")

try:
    while True:
        try:
            response = requests.get(API_URL, timeout=5)
            if response.status_code == 200:
                data = response.json()
                
                # Extract relevant payload or just forward the whole thing
                payload = json.dumps(data).encode('utf-8')
                
                # Produce to Kafka
                producer.produce(topic=topic, value=payload)
                producer.poll(0) # Handle internal delivery callbacks
                
                print(f"📡 Dispatched API data to Kafka at {data['time']['updated']}")
            else:
                print(f"⚠️ API Error: Status code {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection error: {e}")
            
        # Poll every 5 seconds (adjust based on API limits)
        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopping API producer...")
finally:
    producer.flush()
