import json
import redis
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    REDIS_HOST, REDIS_PORT, REDIS_DB, FLINK_PARALLELISM
)


class EnrichTransaction(MapFunction):
    def map(self, value):
        tx = json.loads(value)
        tx["totalAmount"] = round(tx["productPrice"] * tx["productQuantity"], 2)
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
        })
        pipe.expire(f"transactions:{tid}", 86400)
        pipe.zincrby("sales_per_category", amt, tx["productCategory"])
        pipe.zincrby("sales_per_day", amt, day)
        pipe.incr("total_transactions")
        pipe.execute()

        print(f"[Redis] {tid} | {tx['productCategory']} | ${amt}")
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

    stream.map(EnrichTransaction()).map(WriteToRedis())

    print("Starting Flink Commerce Job...")
    env.execute("PyFlink Commerce Analytics")


if __name__ == "__main__":
    main()