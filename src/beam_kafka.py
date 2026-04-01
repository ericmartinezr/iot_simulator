import logging
import sys
import json
import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.kafka import WriteToKafka
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from constants import KAFKA_BROKER, TOPIC

logging.getLogger().setLevel(logging.INFO)


# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/kafkataxi/kafka_taxi.py


def run():
    options = PipelineOptions(
        runner="DirectRunner",
        save_main_session=True,
        streaming=True)

    with beam.Pipeline(options=options) as pipeline:

        kafka = (
            pipeline
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_BROKER,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "False"

                },
                topics=[TOPIC],
                # key_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
                # value_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
                with_metadata=False,
              #  max_num_records=10  # TODO: Remove, for testing only
            )
            | "Extract Value" >> beam.Values()
            | "To JSON" >> beam.Map(json.loads)
            | "Log" >> beam.Map(lambda x: print(x, flush=True))
        )

        kafka

        # (
        #    kafka
        #    # TODO: FixedWindows just for testing, still need to investigate the correct Window strategy for the app
        #    # | "Set Window" >> beam.WindowInto(FixedWindows(15))
        #    | "Write to file" >> WriteToText('file.txt', triggering_frequency=15)
        # )


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"Error {e}")
