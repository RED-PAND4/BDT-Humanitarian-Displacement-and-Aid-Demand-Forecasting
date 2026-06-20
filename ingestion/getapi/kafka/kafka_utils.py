import os
from confluent_kafka.admin import AdminClient, NewTopic


def get_admin_client():
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def create_topic(topic_name, num_partitions=1, replication_factor=1, config=None):
    admin = get_admin_client()
    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor, config=config or {})
    fs = admin.create_topics([topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created")
        except Exception as e:
            if "TopicAlreadyExistsError" in str(e):
                print(f"Topic '{topic}' already exists")
            else:
                raise


def delete_topic(topic_name):
    admin = get_admin_client()
    fs = admin.delete_topics([topic_name])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' deleted")
        except Exception as e:
            raise


def list_topics(timeout=10):
    admin = get_admin_client()
    md = admin.list_topics(timeout=timeout)
    return [topic for topic in md.topics]


def ensure_topics(topics):
    existing = list_topics()
    for topic_def in topics:
        name = topic_def.get("name")
        partitions = topic_def.get("partitions", 1)
        replication = topic_def.get("replication", 1)
        config = topic_def.get("config", {})
        if name not in existing:
            create_topic(name, partitions, replication, config)
        else:
            print(f"Topic '{name}' already exists")
