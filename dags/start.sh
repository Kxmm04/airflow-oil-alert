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

airflow scheduler &
exec airflow webserver --port ${PORT:-8080}