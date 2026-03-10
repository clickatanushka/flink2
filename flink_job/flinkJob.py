import json
import redis
import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
    FLINK_PARALLELISM
)


class EnrichTransaction(MapFunction):
    """Parse raw JSON and compute totalAmount. Also flag fraud."""
    def map(self, value):
        tx = json.loads(value)
        tx["totalAmount"] = round(tx["productPrice"] * tx["productQuantity"], 2)
        # Flag suspicious transactions
        tx["isFraud"] = tx["totalAmount"] > 500
        return tx


class WriteToRedis(MapFunction):
    def open(self, runtime_context):
        self.r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            db=REDIS_DB, decode_responses=True
        )

    def map(self, tx):
        tid = tx["transactionId"]
        day = tx["transactionDate"][:10]
        amt = tx["totalAmount"]

        pipe = self.r.pipeline()

        # Full transaction hash
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

        # Aggregations
        pipe.zincrby("sales_per_category", amt, tx["productCategory"])
        pipe.zincrby("sales_per_day", amt, day)
        pipe.incr("total_transactions")

        # Fraud counter
        if tx["isFraud"]:
            pipe.incr("fraud_count")
            pipe.zincrby("fraud_per_category", amt, tx["productCategory"])

        pipe.execute()
        return tx


class WriteToPostgres(MapFunction):
    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        self.conn.autocommit = True
        self._create_tables()

    def _create_tables(self):
        with self.conn.cursor() as cur:
            # Raw transactions table
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

            # Sales per category table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_per_category (
                    category     VARCHAR PRIMARY KEY,
                    total_sales  FLOAT,
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)

            # Sales per day table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_per_day (
                    sale_date    DATE PRIMARY KEY,
                    total_sales  FLOAT,
                    updated_at   TIMESTAMP DEFAULT NOW()
                )
            """)

    def map(self, tx):
        with self.conn.cursor() as cur:
            # Insert transaction
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

            # Upsert sales per category
            cur.execute("""
                INSERT INTO sales_per_category (category, total_sales)
                VALUES (%s, %s)
                ON CONFLICT (category)
                DO UPDATE SET
                    total_sales = sales_per_category.total_sales + EXCLUDED.total_sales,
                    updated_at = NOW()
            """, (tx["productCategory"], tx["totalAmount"]))

            # Upsert sales per day
            cur.execute("""
                INSERT INTO sales_per_day (sale_date, total_sales)
                VALUES (%s, %s)
                ON CONFLICT (sale_date)
                DO UPDATE SET
                    total_sales = sales_per_day.total_sales + EXCLUDED.total_sales,
                    updated_at = NOW()
            """, (tx["transactionDate"][:10], tx["totalAmount"]))

        print(f"[Postgres] {tx['transactionId']} | ${tx['totalAmount']} | fraud={tx['isFraud']}")
        return tx


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PARALLELISM)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # Pipeline: enrich → write to Redis → write to Postgres
    (
        stream
        .map(EnrichTransaction())
        .map(WriteToRedis())
        .map(WriteToPostgres())
    )

    print("Starting Flink Commerce Job...")
    env.execute("PyFlink Commerce Analytics")


if __name__ == "__main__":
    main()