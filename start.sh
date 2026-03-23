#!/bin/bash
set -e

airflow db migrate

airflow users create \
  --username airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password airflow || true

exec airflow webserver --hostname 0.0.0.0 --port ${PORT:-8080}