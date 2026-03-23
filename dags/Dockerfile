FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags /opt/airflow/dags
COPY start.sh /start.sh
RUN chmod +x /start.sh