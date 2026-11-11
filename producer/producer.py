import json
import random
import time
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

producer = Producer({
    "bootstrap.servers": "localhost:9092",  # Connects to docker container kafka running on localhost:9092
    "acks": "all",                          # Fixed missing comma
    "retries": 3
})

TOPIC_NAME = "raw-transactions"  # Lowercase is Kafka best practice

USERS = [f"U{100+i}" for i in range(30)]  # 30 users so repeat swipes happen quickly during testing
CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Pune", "Mumbai", "Delhi"]


# Delivery Callback (Confirms which partition received the message)
def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        print(
            f"[SENT] Key: {msg.key().decode('utf-8')} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()} | "
            f"Data: {msg.value().decode('utf-8')}"
        )


def generate_transaction():
    user_id = random.choice(USERS)

    # 5% corrupted data to prove our PySpark Dead Letter Queue (DLQ) catches errors
    if random.random() < 0.05:
        return {
            "transaction_id": fake.uuid4(),
            "user_id": user_id,
            "amount": -999,  # Invalid negative amount!
            "currency": "INR",
            "card_type": "credit",
            "city": None,    # Missing city!
            "timestamp": datetime.now().isoformat()
        }

    # 90% normal transactions and 10% fraudulent spikes
    is_spike = random.random() < 0.10
    if is_spike:
        amount = random.randint(10000, 50000)
    else:
        amount = random.randint(10, 1000)

    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "amount": amount,
        "currency": "INR",
        "card_type": random.choice(["credit", "debit"]),
        "city": random.choice(CITIES),
        "timestamp": datetime.now().isoformat()  # Real-time timestamp for accurate velocity checks
    }


if __name__ == "__main__":
    print(f"Starting transaction stream to topic '{TOPIC_NAME}'... Press Ctrl+C to stop.\n")
    try:
        while True:
            txn = generate_transaction()

            producer.produce(
                topic=TOPIC_NAME,
                key=txn["user_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
