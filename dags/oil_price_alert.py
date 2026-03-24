from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import re
import requests
from bs4 import BeautifulSoup

FILE_PATH = "/tmp/last_price.json"


def scrape_price():
    url = "https://www.pttor.com/th/oil_price"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    match = re.search(r"แก๊สโซฮอล์\s*95.*?(\d+\.\d+)", text)
    if match:
        return match.group(1)

    raise ValueError("หาราคาแก๊สโซฮอล์ 95 ไม่เจอ")


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
                "text": msg,
            }
        ]
    }

    res = requests.post(url, headers=headers, json=data, timeout=30)
    res.raise_for_status()


def check_price():
    new_price = scrape_price()

    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            old_price = json.load(f).get("price")
    else:
        old_price = None

    if new_price != old_price:
        msg = f"🚗 ราคาน้ำมันเปลี่ยน!\nเก่า: {old_price}\nใหม่: {new_price}"
        send_line(msg)

    with open(FILE_PATH, "w", encoding="utf-8") as f:
        json.dump({"price": new_price}, f, ensure_ascii=False)


with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["oil", "line"],
) as dag:

    check_price_task = PythonOperator(
        task_id="check_price",
        python_callable=check_price,
    )