import json
from kafka import KafkaConsumer
from constants import TOPIC, KAFKA_BROKER


# For testing the producer and the consumer with Kafka

def run():
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=json.loads
    )
    try:
        kafka_consumer.subscribe([TOPIC])
        for msg in kafka_consumer:
            assert isinstance(msg.value, dict)
            print(msg.value)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    run()
