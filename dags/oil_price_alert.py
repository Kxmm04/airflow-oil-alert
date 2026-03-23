from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("hello airflow")

with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )