import logging
import json
import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions

from constants import KAFKA_BROKER, TOPIC

logging.getLogger().setLevel(logging.INFO)

class ProcessIoTData(beam.PTransform):
    """
    Transforms Kafka value strings into parsed JSON dictionaries.
    """
    def expand(self, pcoll):
        return (
            pcoll
            | "Extract Value" >> beam.Values()
            | "To JSON" >> beam.Map(json.loads)
        )

def run():
    # Configure FlinkRunner locally in streaming mode
    # environment_type="DOCKER" le dice a Flink que corra workers sidecar de Python para este job
    options = PipelineOptions([
        "--runner=FlinkRunner",
        "--flink_master=localhost:8081",
        "--flink_version=1.20",
        "--environment_type=DOCKER",
        "--streaming",
        "--checkpointing_interval=10000",
        "--parallelism=1"
    ])

    with beam.Pipeline(options=options) as pipeline:

        kafka_data = (
            pipeline
            | "Read from Kafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": KAFKA_BROKER,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": "False",
                    "group.id": "iot-beam-group"  # CRITICAL: Ensures offsets coordinate to Python
                },
                topics=[TOPIC],
                with_metadata=False,
                commit_offset_in_finalize=True,  # Forces buffer flush to Python continuously
                #key_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
                #value_deserializer="org.apache.kafka.common.serialization.StringDeserializer"
            )
            | "Process Data" >> ProcessIoTData()
        )

        # Usamos logging en lugar de print() porque bajo FlinkRunner (SDK Worker), print puede ser silenciado o buffereado.
        kafka_data | "Log Real-Time" >> beam.Map(lambda x: logging.info(f"💡 DATO RECIBIDO CORRECTAMENTE EN BEAM: {x}"))

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"Error {e}")
