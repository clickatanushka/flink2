# 🛒 Real-Time E-Commerce Analytics Pipeline

A production-grade real-time data engineering pipeline that processes live e-commerce transactions using Apache Kafka, Python stream processing, Redis, PostgreSQL, and Grafana — deployed on AWS EC2 with ML-powered anomaly detection using Isolation Forest.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LOCAL MACHINE                            │
│                                                                 │
│  ┌─────────────────┐        ┌──────────────────────────────┐   │
│  │   Transaction   │        │      Stream Processor        │   │
│  │    Producer     │        │       (flinkJob.py)          │   │
│  │  (Faker + Kafka │        │                              │   │
│  │   Python)       │        │  • Enrich transactions       │   │
│  └────────┬────────┘        │  • Fraud detection (>$500)   │   │
│           │                 │  • Anomaly detection         │   │
│           │                 │    (Isolation Forest ML)     │   │
│           │                 └──────────────┬───────────────┘   │
└───────────┼──────────────────────────────── ┼ ─────────────────┘
            │                                 │
            ▼                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                         AWS EC2 (t3.small)                      │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐   ┌──────────┐  │
│  │  Apache  │    │  Redis   │    │PostgreSQL│   │ Grafana  │  │
│  │  Kafka   │───▶│ (Cache)  │    │(Storage) │   │Dashboard │  │
│  │          │    │          │    │          │   │          │  │
│  │ Topic:   │    │• tx hash │    │• transac-│   │• Sales   │  │
│  │financial_│    │• sales   │    │  tions   │   │  charts  │  │
│  │transact- │    │  per cat │    │• sales   │   │• Fraud   │  │
│  │ions      │    │• anomaly │    │  per day │   │  detect  │  │
│  │          │    │  counts  │    │• anomaly │   │• Anomaly │  │
│  └──────────┘    └──────────┘    │  table   │   │  rate    │  │
│                                  └──────────┘   └──────────┘  │
└─────────────────────────────────────────────────────────────────┘
```
EC2 instance connect:
<img width="1920" height="1080" alt="Screenshot From 2026-03-11 02-06-43" src="https://github.com/user-attachments/assets/613294d2-545d-430b-bc5d-0f99a6d8df07" />

---

## 🚀 Tech Stack

| Layer | Technology |
|---|---|
| Message Queue | Apache Kafka |
| Stream Processing | Python (Kafka Consumer) |
| ML / Anomaly Detection | Scikit-learn (Isolation Forest) |
| Cache | Redis |
| Permanent Storage | PostgreSQL |
| Visualization | Grafana |
| Infrastructure | AWS EC2 (t3.small) |
| Containerization | Docker + Docker Compose |
| Language | Python 3.11 |

---

## ✨ Features

- **Real-time stream processing** — processes live transactions as they arrive
- **Fraud detection** — flags transactions over $500 as suspicious
- **ML anomaly detection** — Isolation Forest model trains on first 50 transactions, then scores every subsequent transaction for unusual spending patterns
- **Dual storage** — Redis for real-time cache (24h TTL), PostgreSQL for permanent storage
- **Live Grafana dashboard** — 6 panels auto-refreshing every 5 seconds
- **Cloud deployed** — Kafka, Redis, PostgreSQL and Grafana all running on AWS EC2
- **Aggregations** — sales per category, sales per day, anomaly rates tracked in real time


---

## 📊 Grafana Dashboard

The dashboard includes 6 panels:

1. **Sales by Category** — bar chart of revenue per product category
2. **Fraud Detection** — pie chart of fraud vs normal transactions
3. **Revenue per Day** — daily revenue bar chart
4. **Total Transactions** — live running count
5. **Detected Anomalies** — table of ML-flagged transactions with scores
6. **Anomaly Rate** — pie chart of anomaly vs normal transactions

DashBoard View 1.
<img width="1920" height="1080" alt="Screenshot From 2026-03-11 20-11-36" src="https://github.com/user-attachments/assets/5cd29f4e-d5f3-4e27-8a16-b13f6b30636b" />

Dashboard View 2.
<img width="1920" height="1080" alt="Screenshot From 2026-03-11 20-11-46" src="https://github.com/user-attachments/assets/3aaf17e1-ffe9-4cf1-a1b1-105a32ba8833" />


---

## 🗄️ Data Schema

### `transactions`
| Column | Type | Description |
|---|---|---|
| transaction_id | VARCHAR | Unique ID |
| product_name | VARCHAR | Product name |
| product_category | VARCHAR | Category |
| product_price | FLOAT | Unit price |
| product_quantity | INT | Quantity |
| total_amount | FLOAT | Price × Quantity |
| customer_id | VARCHAR | Customer ID |
| payment_method | VARCHAR | Payment type |
| transaction_date | TIMESTAMP | When it happened |
| is_fraud | BOOLEAN | Rule-based flag (>$500) |
| anomaly_score | FLOAT | ML anomaly score |
| is_anomaly | BOOLEAN | ML anomaly flag |

### `sales_per_category`
| Column | Type | Description |
|---|---|---|
| category | VARCHAR | Product category |
| total_sales | FLOAT | Cumulative revenue |
| updated_at | TIMESTAMP | Last updated |

### `anomalies`
| Column | Type | Description |
|---|---|---|
| transaction_id | VARCHAR | Transaction reference |
| customer_id | VARCHAR | Customer reference |
| product_category | VARCHAR | Category |
| total_amount | FLOAT | Transaction amount |
| anomaly_score | FLOAT | Isolation Forest score |
| detected_at | TIMESTAMP | Detection time |

---

## 📁 Project Structure

```
flink-commerce-py/
├── config/
│   ├── __init__.py
│   └── settings.py              # All configuration
├── producer/
│   └── transaction_producer.py  # Generates fake transactions → Kafka
├── flink_job/
│   └── flinkJob.py              # Stream processor + ML anomaly detection
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── postgres.yaml    # Auto-provisions Postgres datasource
├── docker-compose.yml           # EC2 services (Kafka, Redis, Postgres, Grafana)
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup & Run

### Prerequisites
- Python 3.11
- Docker + Docker Compose
- AWS EC2 instance (t3.small or higher)
- Java 17

### 1. Clone the repo
```bash
git clone https://github.com/clickatanushka/flink2.git
cd flink-commerce-py
```

### 2. Set up Python environment
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure settings
Edit `config/settings.py` with your EC2 public IP:
```python
KAFKA_BOOTSTRAP_SERVERS = "YOUR_EC2_IP:9092"
REDIS_HOST = "YOUR_EC2_IP"
POSTGRES_HOST = "YOUR_EC2_IP"
```

### 4. Start EC2 services
```bash
# SSH into EC2
ssh -i your-key.pem ubuntu@YOUR_EC2_IP
cd ~/flink2
sudo docker-compose up -d
```

### 5. Run the pipeline
```bash
# Terminal 1 - Stream processor with ML
source venv/bin/activate
python flink_job/flinkJob.py

# Terminal 2 - Transaction producer
source venv/bin/activate
python producer/transaction_producer.py
```

### 6. View dashboards
| Service | URL |
|---|---|
| Grafana | http://YOUR_EC2_IP:3000 |
| Redis Commander | http://YOUR_EC2_IP:8082 |

---

## 🤖 ML Model — Isolation Forest

The anomaly detection uses **Isolation Forest**, an unsupervised ML algorithm that:

- Requires **no labeled training data**
- Detects outliers by isolating observations in random decision trees
- Trains on the first 50 transactions automatically
- Retrains every 100 new transactions to adapt to changing patterns
- Scores each transaction — lower scores indicate higher anomaly likelihood

**Features used for detection:**
- Product price
- Quantity ordered
- Total amount
- Product category (encoded)
- Payment method (encoded)

  Terminal View:
<img width="1920" height="1080" alt="Screenshot From 2026-03-11 20-08-44" src="https://github.com/user-attachments/assets/c682e1dc-91d8-41f9-8da2-cbad0060f4b9" />

---

## 📈 Sample Output

```
Connecting to Redis and Postgres...
Connected!
Listening on topic: financial_transactions
Collecting 50 transactions before training model...
Training Isolation Forest model...
Model trained! Now detecting anomalies...
[✅ normal] abc-1234 | Electronics | $299.99 | score=-0.4821
[✅ normal] def-5678 | Clothing   | $45.00  | score=-0.4923
[🚨 ANOMALY] ghi-9012 | Toys      | $8934.00| score=-0.8234
[✅ normal] jkl-3456 | Beauty     | $120.00 | score=-0.5102
```

---

## 🔧 requirements.txt

```
apache-flink==1.18.0
kafka-python==2.0.2
redis==5.0.1
faker==20.1.0
py4j==0.10.9.7
python-dateutil>=2.8.0
psycopg2-binary
scikit-learn
numpy<2.0
grpcio
apache-beam==2.48.0
cloudpickle
pandas
pyarrow==11.0.0
```

---

## 🙋 Author

Built by Anushka — third year CS student passionate about data engineering and real-time systems.

---

## ⭐ If you found this useful, give it a star!
