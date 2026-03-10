import json
import redis
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = "13.127.178.157:9092"
KAFKA_TOPIC = "financial_transactions"
KAFKA_GROUP_ID = "flink-commerce-group"
REDIS_HOST = "13.127.178.157"
POSTGRES_HOST = "13.127.178.157"


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
                is_fraud         BOOLEAN
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


def enrich(tx):
    tx["totalAmount"] = round(tx["productPrice"] * tx["productQuantity"], 2)
    tx["isFraud"] = tx["totalAmount"] > 500
    return tx


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
    })
    pipe.expire(f"transactions:{tid}", 86400)
    pipe.zincrby("sales_per_category", amt, tx["productCategory"])
    pipe.zincrby("sales_per_day", amt, day)
    pipe.incr("total_transactions")
    if tx["isFraud"]:
        pipe.incr("fraud_count")
        pipe.zincrby("fraud_per_category", amt, tx["productCategory"])
    pipe.execute()


def write_postgres(conn, tx):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO transactions
                (transaction_id, product_name, product_category,
                 product_price, product_quantity, total_amount,
                 customer_id, payment_method, transaction_date, is_fraud)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            tx["transactionId"], tx["productName"], tx["productCategory"],
            tx["productPrice"], tx["productQuantity"], tx["totalAmount"],
            tx["customerId"], tx["paymentMethod"], tx["transactionDate"],
            tx["isFraud"]
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


def main():
    print("Connecting to Redis and Postgres...")
    r = get_redis()
    conn = get_postgres()
    create_tables(conn)
    print("Connected! Starting consumer...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
    for message in consumer:
        tx = enrich(message.value)
        write_redis(r, tx)
        write_postgres(conn, tx)
        print(f"[OK] {tx['transactionId']} | {tx['productCategory']} | ${tx['totalAmount']} | fraud={tx['isFraud']}")


if __name__ == "__main__":
    main()