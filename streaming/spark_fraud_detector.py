import json
from datetime import datetime
import redis
import psycopg2
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session with Kafka & PostgreSQL packages
spark = (
    SparkSession.builder
    .appName("RealTimeFraudDetection")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2. Define Expected JSON Schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Read Stream from Kafka (using internal Docker address kafka:29092)
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# Parse raw Kafka bytes into structured columns
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) as raw_json")
    .withColumn("data", from_json(col("raw_json"), transaction_schema))
    .select("raw_json", "data.*")
)


# 4. Micro-Batch Processor: Validation + Redis State Lookup + Fraud Rules + Postgres Sink
def process_micro_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    # Connect to Redis, Postgres, and Kafka Producer inside Docker network
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    kafka_producer = Producer({"bootstrap.servers": "kafka:29092"})
    pg_conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="fraud_db",
        user="admin",
        password="password123"
    )
    cursor = pg_conn.cursor()

    for row in rows:
        # A. Data Quality Check -> Route broken records to Dead Letter Queue (DLQ)
        if not row.transaction_id or not row.user_id or not row.city or row.amount is None or row.amount <= 0:
            print(f"[DLQ] Routing corrupted message: {row.raw_json}")
            kafka_producer.produce("transactions-dlq", value=row.raw_json.encode("utf-8"))
            continue

        is_fraud = False
        reasons = []

        # B. Rule 1: High-Amount Spike (> ₹20,000)
        if row.amount > 20000:
            is_fraud = True
            reasons.append(f"High amount spike (₹{row.amount})")

        # C. Rule 2: Impossible Travel Check via Redis State Cache
        redis_key = f"user_state:{row.user_id}"
        last_state = r.hgetall(redis_key)

        if last_state:
            last_city = last_state.get("city")
            last_time = datetime.fromisoformat(last_state.get("timestamp"))
            curr_time = datetime.fromisoformat(row.timestamp)
            seconds_diff = abs((curr_time - last_time).total_seconds())

            # Different city within 5 minutes (300 seconds) = Impossible Travel
            if last_city and last_city != row.city and seconds_diff < 300:
                is_fraud = True
                reasons.append(f"Impossible travel ({last_city} -> {row.city} in {int(seconds_diff)}s)")

        # Update Redis with user's latest city & timestamp (expires after 1 hour)
        r.hset(redis_key, mapping={"city": row.city, "timestamp": row.timestamp})
        r.expire(redis_key, 3600)

        fraud_reason = " | ".join(reasons) if reasons else None

        # D. Insert into PostgreSQL (all_transactions)
        cursor.execute(
            """
            INSERT INTO all_transactions 
            (transaction_id, user_id, amount, currency, card_type, city, event_timestamp, is_fraud, fraud_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
            """,
            (row.transaction_id, row.user_id, row.amount, row.currency, row.card_type,
             row.city, row.timestamp, is_fraud, fraud_reason)
        )

        # E. If Fraudulent -> Save to fraud_cases table & publish to 'fraud-alerts' topic
        if is_fraud:
            print(f"[FRAUD ALERT] User {row.user_id} | ₹{row.amount} in {row.city} | Reason: {fraud_reason}")
            cursor.execute(
                """
                INSERT INTO fraud_cases (transaction_id, user_id, amount, city, fraud_reason)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (row.transaction_id, row.user_id, row.amount, row.city, fraud_reason)
            )
            alert_payload = json.dumps({
                "transaction_id": row.transaction_id,
                "user_id": row.user_id,
                "amount": row.amount,
                "city": row.city,
                "reason": fraud_reason
            })
            kafka_producer.produce("fraud-alerts", key=row.user_id.encode("utf-8"), value=alert_payload.encode("utf-8"))
        else:
            print(f"[OK] User {row.user_id} | ₹{row.amount} in {row.city}")

    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    kafka_producer.flush()


# 5. Start the Streaming Query (Processes micro-batches every 2 seconds)
query = (
    parsed_stream.writeStream
    .foreachBatch(process_micro_batch)
    .trigger(processingTime="2 seconds")
    .start()
)

query.awaitTermination()
