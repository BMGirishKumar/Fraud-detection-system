import json
from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",  # Connects to Docker Kafka on localhost:9092
    "group.id": "fraud-alert-notifier-group",
    "auto.offset.reset": "earliest",        # Fixed missing comma
    "enable.auto.commit": False             # Manually commit offsets after alert is handled
}

consumer = Consumer(conf)
consumer.subscribe(["fraud-alerts"])        # Listens for confirmed fraud from PySpark

print("Listening for live FRAUD ALERTS... Press Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"[ERROR] Consumer error: {msg.error()}")
            continue

        key = msg.key().decode("utf-8") if msg.key() else "N/A"
        alert = json.loads(msg.value().decode("utf-8"))

        print(
            f"🚨 [FRAUD ALERT] User: {key} | "
            f"Amount: ₹{alert['amount']} | "
            f"City: {alert['city']} | "
            f"Reason: {alert['reason']}"
        )

        # Manually commit the offset only after processing the alert
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    print("\nStopping alert consumer...")
finally:
    consumer.close()
