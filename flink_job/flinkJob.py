import json
import redis
import psycopg2
import numpy as np
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest
from collections import deque

KAFKA_BOOTSTRAP_SERVERS = "13.127.178.157:9092"
KAFKA_TOPIC = "financial_transactions"
KAFKA_GROUP_ID = "flink-commerce-group"
REDIS_HOST = "13.127.178.157"
POSTGRES_HOST = "13.127.178.157"

# How many transactions to collect before training
TRAINING_SIZE = 50


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)


def get_postgres():
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=5432,
        dbname="ecommerce", user="flink", password="flink123"
    )
    conn.autocommit = True
    return conn


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id   VARCHAR PRIMARY KEY,
                product_name     VARCHAR,
                product_category VARCHAR,
                product_price    FLOAT,
                product_quantity INT,
                total_amount     FLOAT,
                customer_id      VARCHAR,
                payment_method   VARCHAR,
                transaction_date TIMESTAMP,
                is_fraud         BOOLEAN,
                anomaly_score    FLOAT DEFAULT 0,
                is_anomaly       BOOLEAN DEFAULT FALSE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_per_category (
                category     VARCHAR PRIMARY KEY,
                total_sales  FLOAT,
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_per_day (
                sale_date    DATE PRIMARY KEY,
                total_sales  FLOAT,
                updated_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                transaction_id   VARCHAR PRIMARY KEY,
                customer_id      VARCHAR,
                product_category VARCHAR,
                total_amount     FLOAT,
                anomaly_score    FLOAT,
                detected_at      TIMESTAMP DEFAULT NOW()
            )
        """)


def enrich(tx):
    tx["totalAmount"] = round(tx["productPrice"] * tx["productQuantity"], 2)
    tx["isFraud"] = tx["totalAmount"] > 500
    return tx


def extract_features(tx):
    """Extract numerical features for anomaly detection."""
    category_map = {
        "Electronics": 0, "Clothing": 1, "Home & Garden": 2,
        "Sports": 3, "Books": 4, "Toys": 5, "Beauty": 6
    }
    payment_map = {
        "credit_card": 0, "debit_card": 1, "paypal": 2, "crypto": 3
    }
    return [
        tx["productPrice"],
        tx["productQuantity"],
        tx["totalAmount"],
        category_map.get(tx["productCategory"], -1),
        payment_map.get(tx["paymentMethod"], -1)
    ]


def write_redis(r, tx):
    tid = tx["transactionId"]
    day = tx["transactionDate"][:10]
    amt = tx["totalAmount"]
    pipe = r.pipeline()
    pipe.hset(f"transactions:{tid}", mapping={
        "transactionId":   tid,
        "productName":     tx["productName"],
        "productCategory": tx["productCategory"],
        "productPrice":    str(tx["productPrice"]),
        "productQuantity": str(tx["productQuantity"]),
        "totalAmount":     str(amt),
        "customerId":      tx["customerId"],
        "paymentMethod":   tx["paymentMethod"],
        "transactionDate": tx["transactionDate"],
        "isFraud":         str(tx["isFraud"]),
        "anomalyScore":    str(tx.get("anomalyScore", 0)),
        "isAnomaly":       str(tx.get("isAnomaly", False)),
    })
    pipe.expire(f"transactions:{tid}", 86400)
    pipe.zincrby("sales_per_category", amt, tx["productCategory"])
    pipe.zincrby("sales_per_day", amt, day)
    pipe.incr("total_transactions")
    if tx.get("isAnomaly"):
        pipe.incr("anomaly_count")
        pipe.zincrby("anomalies_per_category", amt, tx["productCategory"])
    pipe.execute()


def write_postgres(conn, tx):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transactions
                (transaction_id, product_name, product_category,
                 product_price, product_quantity, total_amount,
                 customer_id, payment_method, transaction_date,
                 is_fraud, anomaly_score, is_anomaly)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            tx["transactionId"], tx["productName"], tx["productCategory"],
            tx["productPrice"], tx["productQuantity"], tx["totalAmount"],
            tx["customerId"], tx["paymentMethod"], tx["transactionDate"],
            tx["isFraud"], tx.get("anomalyScore", 0), tx.get("isAnomaly", False)
        ))
        cur.execute("""
            INSERT INTO sales_per_category (category, total_sales)
            VALUES (%s, %s)
            ON CONFLICT (category)
            DO UPDATE SET
                total_sales = sales_per_category.total_sales + EXCLUDED.total_sales,
                updated_at = NOW()
        """, (tx["productCategory"], tx["totalAmount"]))
        cur.execute("""
            INSERT INTO sales_per_day (sale_date, total_sales)
            VALUES (%s, %s)
            ON CONFLICT (sale_date)
            DO UPDATE SET
                total_sales = sales_per_day.total_sales + EXCLUDED.total_sales,
                updated_at = NOW()
        """, (tx["transactionDate"][:10], tx["totalAmount"]))

        if tx.get("isAnomaly"):
            cur.execute("""
                INSERT INTO anomalies
                    (transaction_id, customer_id, product_category,
                     total_amount, anomaly_score)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, (
                tx["transactionId"], tx["customerId"],
                tx["productCategory"], tx["totalAmount"],
                tx.get("anomalyScore", 0)
            ))


def main():
    print("Connecting to Redis and Postgres...")
    r = get_redis()
    conn = get_postgres()
    create_tables(conn)
    print("Connected!")

    # Isolation Forest model
    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,  # expect 10% anomalies
        random_state=42
    )
    training_buffer = deque(maxlen=TRAINING_SIZE)
    model_trained = False

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print(f"Listening on topic: {KAFKA_TOPIC}")
    print(f"Collecting {TRAINING_SIZE} transactions before training model...")

    for message in consumer:
        tx = enrich(message.value)
        features = extract_features(tx)
        training_buffer.append(features)

        if not model_trained:
            if len(training_buffer) >= TRAINING_SIZE:
                print("Training Isolation Forest model...")
                model.fit(list(training_buffer))
                model_trained = True
                print("Model trained! Now detecting anomalies...")
            tx["anomalyScore"] = 0
            tx["isAnomaly"] = False
        else:
            score = model.score_samples([features])[0]
            is_anomaly = model.predict([features])[0] == -1
            tx["anomalyScore"] = round(float(score), 4)
            tx["isAnomaly"] = bool(is_anomaly)

            # Retrain every 100 transactions
            if len(training_buffer) % 100 == 0:
                model.fit(list(training_buffer))

        write_redis(r, tx)
        write_postgres(conn, tx)

        status = "🚨 ANOMALY" if tx.get("isAnomaly") else "✅ normal"
        print(f"[{status}] {tx['transactionId'][:8]} | {tx['productCategory']} | ${tx['totalAmount']} | score={tx.get('anomalyScore', 0)}")


if __name__ == "__main__":
    main()