from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource

import json


def parse_event(event):
    data = json.loads(event)

    return (
        data["productID"],
        data["productName"],
        data["totalAmount"]
    )


def main():

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("financial_transactions") \
        .set_group_id("flink-consumer") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    parsed = stream.map(parse_event)

    keyed = parsed.key_by(lambda x: x[0])

    aggregated = keyed.reduce(
        lambda a, b: (a[0], a[1], a[2] + b[2])
    )

    aggregated.print()

    env.execute("Flink Streaming Sales Job")


if __name__ == "__main__":
    main()