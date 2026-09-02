# Test Kafka consumer settings

from app import kafka_consumer

def test_kafka_consumer_settings():
    # Check the Kafka connection settings
    assert kafka_consumer.KAFKA_SERVER == "localhost:9092"
    assert kafka_consumer.KAFKA_TOPIC == "ecommerce-transactions"
    assert kafka_consumer.KAFKA_GROUP_ID == "icestream-data-quality"