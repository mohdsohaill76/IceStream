import os
import urllib.request

JAR_URL = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
LIB_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET_JAR = os.path.join(LIB_DIR, "flink-sql-connector-kafka-3.0.1-1.18.jar")


def ensure_kafka_jar_exists():
    """Ensures the required Flink Kafka connector JAR is present locally."""
    if not os.path.exists(TARGET_JAR):
        print(f"Downloading Flink Kafka Connector JAR from Maven Central...\nURL: {JAR_URL}")
        try:
            urllib.request.urlretrieve(JAR_URL, TARGET_JAR)
            print(f"Successfully downloaded connector JAR to: {TARGET_JAR}")
        except Exception as e:
            print(f"Failed to download Kafka connector JAR: {e}")
            raise e
    else:
        print(f"Kafka Connector JAR already present at: {TARGET_JAR}")


if __name__ == "__main__":
    ensure_kafka_jar_exists()