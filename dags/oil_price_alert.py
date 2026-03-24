from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup

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
        "messages": [
            {
                "type": "text",
                "text": msg[:4900],
            }
        ]
    }

    res = requests.post(url, headers=headers, json=data, timeout=30)
    res.raise_for_status()

def check_price():
    url = "https://gasprice.kapook.com/gasprice.php"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()

    text = BeautifulSoup(res.text, "html.parser").get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    matched_lines = [line for line in lines if "95" in line or "ปตท" in line or "PTT" in line]

    debug_text = "DEBUG KAPOOK\n\n" + "\n".join(matched_lines[:30])
    send_line(debug_text)

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