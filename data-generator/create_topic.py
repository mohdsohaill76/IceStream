import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
KAFKA_REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))


def create_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER,
        client_id="icestream-topic-admin",
    )

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=KAFKA_PARTITIONS,
        replication_factor=KAFKA_REPLICATION_FACTOR,
    )

    try:
        admin_client.create_topics(
            new_topics=[topic],
            validate_only=False,
        )
        print(f"Kafka topic '{KAFKA_TOPIC}' created successfully.")

    except TopicAlreadyExistsError:
        print(f"Kafka topic '{KAFKA_TOPIC}' already exists.")

    finally:
        admin_client.close()


if __name__ == "__main__":
    create_topic()
