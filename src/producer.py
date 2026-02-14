import random
import time
from kafka import KafkaProducer
from randomtimestamp import randomtimestamp

# Localhost since this python script runs in local
KAFKA_BROKER = "localhost:9092"
TOPIC = "iot-topic"


def run():
    # Sensor ID
    sensor_id = random.randint(1, 100)

    # Temperature
    temperature = round(random.uniform(15.0, 30.0), 2)

    # Humidity
    humidity = random.uniform(30.0, 90.0)

    # Timestamp
    timestamp = randomtimestamp(
        start_year=2024, end_year=2026, text=True, pattern="%Y-%m-%dT%H:%M:%SZ")

    # TODO: Loop While True and sleep
    # TODO: Sometimes "sleep" for longer and accumulate data to send in batches, it normally should send one at a time
    # TODO: Connect to Pub/Sub and send the data
    # TODO: Add random data with late data (timestamp in the past) to simulate late data arrival

    fake_sensor_data = {
        "device_id": f"sensor_{sensor_id:03d}",
        "temperature": temperature,
        "humidity": humidity,
        "timestamp": timestamp
    }

    print(fake_sensor_data)

    return fake_sensor_data


if __name__ == "__main__":
    sleep_time = random.uniform(2.5, 3.0)
    acumulator = random.randint(1, 10)
    acumulator_count = 0
    fake_data = []

    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    try:

        while True:

            while acumulator_count < acumulator:
                fake_data.append(run())
                acumulator_count += 1

            # TODO: Add send to pub/sub here
            for fdata in fake_data:
                kafka_producer.send(TOPIC, value=str(fdata).encode("utf-8"))

            fake_data = []
            acumulator_count = 0
            time.sleep(sleep_time)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        kafka_producer.flush()
        kafka_producer.close()
