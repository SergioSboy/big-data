import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = "raw_reviews"
KAFKA_BOOTSTRAP = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # default=str
)

df = pd.read_csv("/app/data/reviews.csv")

print(f"Loaded {len(df)} records")

for _, row in df.iterrows():
    message = {
        "review_id": int(row["review_id"]),
        "text": row["text"],
        "rating": int(row["rating"]),
        "timestamp": row["timestamp"]
    }

    producer.send(KAFKA_TOPIC, message)
    print("Sent:", message["review_id"])

    time.sleep(random.uniform(0.2, 1.0))  # real-time имитация
