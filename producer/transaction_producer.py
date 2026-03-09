import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Beauty"]
PRODUCTS = {
    "Electronics":   ["Laptop", "Phone", "Headphones", "Tablet", "Camera"],
    "Clothing":      ["T-Shirt", "Jeans", "Jacket", "Shoes", "Hat"],
    "Home & Garden": ["Sofa", "Lamp", "Plant Pot", "Rug", "Curtains"],
    "Sports":        ["Dumbbells", "Yoga Mat", "Running Shoes", "Bicycle", "Helmet"],
    "Books":         ["Fiction Novel", "Tech Book", "Cook Book", "Biography", "Comic"],
    "Toys":          ["Lego Set", "Board Game", "Action Figure", "Puzzle", "Doll"],
    "Beauty":        ["Perfume", "Lipstick", "Moisturizer", "Shampoo", "Foundation"],
}

def generate_transaction():
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    return {
        "transactionId":    fake.uuid4(),
        "productId":        f"PROD-{random.randint(1000, 9999)}",
        "productName":      product,
        "productCategory":  category,
        "productPrice":     round(random.uniform(5.0, 1000.0), 2),
        "productQuantity":  random.randint(1, 10),
        "productBrand":     fake.company(),
        "currency":         "USD",
        "customerId":       f"CUST-{random.randint(1, 500)}",
        "transactionDate":  datetime.now().isoformat(),
        "paymentMethod":    random.choice(["credit_card", "debit_card", "paypal", "crypto"]),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Producing to topic: {KAFKA_TOPIC}")
    while True:
        tx = generate_transaction()
        producer.send(KAFKA_TOPIC, value=tx)
        print(f"Sent: {tx['transactionId']} | {tx['productCategory']} | ${tx['productPrice']}")
        time.sleep(1)

if __name__ == "__main__":
    main()