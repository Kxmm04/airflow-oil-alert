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
    res.encoding = res.apparent_encoding

    text = BeautifulSoup(res.text, "html.parser").get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    out = ["DEBUG KAPOOK"]
    for i, line in enumerate(lines):
        if "แก๊สโซฮอล์ 95" in line or "ราคาน้ำมัน ปตท." in line or "ราคานํ้ามัน ปตท." in line:
            start = max(0, i - 3)
            end = min(len(lines), i + 6)
            out.append(f"\n--- BLOCK around line {i} ---")
            for j in range(start, end):
                out.append(f"{j}: {lines[j]}")

    debug_text = "\n".join(out)
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