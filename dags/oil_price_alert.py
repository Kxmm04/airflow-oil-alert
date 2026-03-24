from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import traceback

def send_line(msg: str):
    token = os.getenv("LINE_TOKEN")
    if not token:
        raise ValueError("ไม่พบ LINE_TOKEN")

    url = "https://api.line.me/v2/bot/message/broadcast"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }
    data = {
        "messages": [{"type": "text", "text": msg}]
    }

    res = requests.post(url, headers=headers, json=data, timeout=30)
    print("LINE status:", res.status_code)
    print("LINE body:", res.text)
    res.raise_for_status()

def check_price():
    try:
        print("START check_price")
        send_line("✅ ทดสอบ Airflow + LINE สำเร็จ")
        print("END check_price")
    except Exception as e:
        print("ERROR IN check_price:", repr(e))
        traceback.print_exc()
        raise

with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="check_price",
        python_callable=check_price
    )