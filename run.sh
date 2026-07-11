#!/usr/bin/env bash
# One-shot demo: bring up infra, register the topic, produce, consume.
set -euo pipefail
cd "$(dirname "$0")"

PY=python
[ -x .venv/bin/python ] && PY=.venv/bin/python

echo "==> Resetting cluster (wipes all MinIO + Postgres data)..."
docker compose down -v

echo "==> Starting MinIO, Postgres, brokers..."
docker compose up -d

echo "==> Waiting for broker on localhost:9092..."
# ponytail: dumb TCP poll, 30s cap; swap for a healthcheck if flaky
for i in $(seq 1 30); do
  if (exec 3<>/dev/tcp/localhost/9092) 2>/dev/null; then break; fi
  sleep 1
done

# Topics must be registered before producing, else messages route nowhere.
# examples/create_topic.py runs on the host, so it needs MinIO's published port (9010->9000).
echo "==> Registering 'demo-topic'..."
MINIO_ENDPOINT=localhost:9010 "$PY" examples/create_topic.py demo-topic 2 || true

# Brokers refresh the topic config from MinIO on a 5s cache; wait it out.
sleep 6

echo "==> Producing..."
"$PY" examples/producer.py

echo "==> Consuming..."
"$PY" examples/consumer.py
