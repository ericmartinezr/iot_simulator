FROM flink:1.20
USER root

# Instalar Docker CLI para que Flink (Beam) pueda interactuar con el Daemon montado
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
