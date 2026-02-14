# iot_simulator_gcp

# Kafka

## Docker

```sh
docker compose up -d
docker exec --workdir /opt/kafka/bin/ -it broker sh

# Crea el topico
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic iot-topic

# Producer en consola que viene por defecto con Kafka
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot-topic

# Consumer en consola que viene por defecto con Kafka
# Permite leer el contenido enviado por el Producer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-topic --from-beginning
```

## Referencia

- https://kafka.apache.org/quickstart/
- https://github.com/dpkp/kafka-python
- https://hub.docker.com/r/apache/kafka
