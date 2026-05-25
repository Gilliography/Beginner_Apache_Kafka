from kafka import KafkaProducer
import json
import time

# Wait until Kafka is ready
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        print("Connected to Kafka!")
        break

    except Exception as e:
        print("Kafka not ready yet. Retrying in 5 seconds...")
        time.sleep(5)

topic = "test-topic"

print("Start typing messages...")
print("Type 'exit' to quit.\n")

while True:
    message = input("Enter message: ")

    if message.lower() == "exit":
        break

    data = {
        "message": message
    }

    producer.send(topic, value=data)
    producer.flush()

    print(f"Sent: {data}")