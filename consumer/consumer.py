import json
from confluent_kafka import Consumer, KafkaError

conf={
    "bootstrap.servers": "localhost:9092",  # Connects to docker container kafka running on localhost:9092
    "group.id": "fraud-detection-consumer-group",
    "auto.offset.reset": "earliest"  # Start reading from the earliest message in the topic
    "enable.auto.commit": False  # Disable auto-commit to manually control offset commits
}

consumer=Consumer(conf)

consumer.subscribe(["raw-transactions"]) # Subscribe to the topic

print("Starting consumer... Press Ctrl+C to stop.\n")

try:
    while True:
        msg=consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

        if msg is None:
            continue  # No message received, continue polling
        if msg.error():
            print(f"[ERROR] Consumer error: {msg.error()}")
            continue
        # Decode key and JSON value from bytes
        key = msg.key().decode("utf-8")
        txn = json.loads(msg.value().decode("utf-8"))

        print(
            f"[RECEIVED] Partition: {msg.partition()} | "
            f"Offset: {msg.offset()} | "
            f"User: {key} | "
            f"Amount: ₹{txn['amount']} | "
            f"City: {txn['city']}"
        )

        # Manually commit the offset ONLY after we finish reading/processing
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()