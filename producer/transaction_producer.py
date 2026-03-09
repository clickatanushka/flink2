from faker import Faker
from confluent_kafka import Producer
from datetime import datetime
import random
import time
import json

fake = Faker()

# Kafka configuration
conf = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(conf)

TOPIC = "financial_transactions"


def generate_transaction():

    user = fake.simple_profile()

    transaction = {
        "transactionID": fake.uuid4(),
        "productID": random.choice(
            ["product1", "product2", "product3", "product4", "product5"]
        ),
        "productName": random.choice(
            ["laptop", "mobile", "tablet", "watch", "headphones"]
        ),
        "productCategory": random.choice(
            ["electronics", "fashion", "home", "sports", "beauty"]
        ),
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 5),
        "productBrand": random.choice(
            ["apple", "samsung", "oneplus", "sony", "boat"]
        ),
        "currency": "USD",
        "customerID": user["username"],
        "transactionDate": datetime.utcnow().isoformat(),
        "paymentMethod": random.choice(
            ["credit_card", "debit_card", "online"]
        )
    }

    transaction["totalAmount"] = (
        transaction["productPrice"] * transaction["productQuantity"]
    )

    return transaction


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Sent to {msg.topic()} partition {msg.partition()}"
        )


def main():

    print("Producer started...")

    topic = "financial_transactions"

    while True:
        print("Generating transaction...")

   

        transaction = generate_transaction()

        producer.produce(
            TOPIC,
            key=transaction["transactionID"],
            value=json.dumps(transaction),
            callback=delivery_report
        )

        producer.poll(0)

        print(transaction)

        time.sleep(2)


if __name__ == "__main__":
    main()