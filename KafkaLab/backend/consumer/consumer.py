import json
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from model import SentimentModel

WINDOW_SIZE = 200
stats_file = "/data/stats.json"

buffer = deque(maxlen=WINDOW_SIZE)

consumer = KafkaConsumer(
    "raw_reviews",
    bootstrap_servers="kafka:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # default=str
)

model = SentimentModel()

for msg in consumer:
    data = msg.value
    sentiment, conf = model.predict(data["text"])

    processed = {
        **data,
        "sentiment": sentiment,
        "confidence": conf
    }

    producer.send("processed_reviews", processed)
    buffer.append(processed)

    pos = sum(1 for x in buffer if x["sentiment"] == "positive")
    neg = len(buffer) - pos

    stats = {
        "window": len(buffer),
        "positive": pos,
        "negative": neg,
        "positive_pct": round(pos / len(buffer) * 100, 2)
    }

    with open(stats_file, "w") as f:
        json.dump(stats, f)
