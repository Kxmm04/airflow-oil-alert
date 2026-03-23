FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./dags /opt/airflow/dags
COPY --chmod=755 start.sh /start.sh