import unittest
import json
import logging
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from beam_kafka import ProcessIoTData

class IotSimulatorBeamTest(unittest.TestCase):
    def test_process_iot_data(self):
        # We simulate the (key, payload) emitted by ReadFromKafka
        expected_output = [
            {"device_id": "sensor_001", "temperature": 25.5, "humidity": 50.0, "timestamp": "2023-10-01T12:00:00+00:00"}
        ]
        
        # Serialize to STRINGs exactly as Kafka would provide via StringDeserializer
        input_data = [
            ("key_1", json.dumps(expected_output[0]))
        ]
        
        with TestPipeline() as pipeline:
            result = (
                pipeline
                | "Create mock Kafka messages" >> beam.Create(input_data)
                | "Process Data" >> ProcessIoTData()
            )
            
            # Assert that the result matches the expected JSON objects
            assert_that(result, equal_to(expected_output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
