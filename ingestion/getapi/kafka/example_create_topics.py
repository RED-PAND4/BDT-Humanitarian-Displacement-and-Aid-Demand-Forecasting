from kafka.kafka_utils import ensure_topics

if __name__ == "__main__":
    topics = [
        {"name": "input-events", "partitions": 3, "replication": 1},
        {"name": "processed-events", "partitions": 3, "replication": 1},
    ]
    ensure_topics(topics)
