import random
import time
from randomtimestamp import randomtimestamp


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
    sleep_time = random.uniform(1.5, 3.0)
    acumulator = random.randint(1, 10)
    acumulator_count = 0
    fake_data = []
    while True:

        while acumulator_count < acumulator:
            fake_data.append(run())
            acumulator_count += 1

        # TODO: Add send to pub/sub here
        # send_to_pubsub(fake_data)

        fake_data = []
        acumulator_count = 0
        time.sleep(sleep_time)
