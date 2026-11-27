# Image commune aux services streaming, producteur et api : Python 3.11 + Java 17 + PySpark 3.5.
# Base Debian bookworm (pas trixie) car c'est elle qui fournit OpenJDK 17.
FROM python:3.11-slim-bookworm

# Java 17 pour Spark ; procps fournit "ps", utilisé par les scripts de lancement de Spark
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 PYTHONUTF8=1

WORKDIR /app
# Dépendances d'abord : cette couche reste en cache tant que requirements.txt ne change pas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# data/ est monté en volume partagé par docker compose (voir .dockerignore)
