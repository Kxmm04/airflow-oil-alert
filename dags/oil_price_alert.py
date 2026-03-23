from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import os
import json
import re

FILE_PATH = "/tmp/last_price.json"

def scrape_price():
    url = "https://www.pttor.com/th/oil_price"
    
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    text = soup.get_text()

    match = re.search(r"แก๊สโซฮอล์ 95.*?(\d+\.\d+)", text)

    if match:
        return match.group(1)
    else:
        return "0"

def send_line(msg):
    token = os.getenv("LINE_TOKEN")

    url = "https://api.line.me/v2/bot/message/broadcast"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    data = {
        "messages": [
            {
                "type": "text",
                "text": msg
            }
        ]
    }

    requests.post(url, headers=headers, json=data)

def check_price():
    new_price = scrape_price()

    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r") as f:
            old_price = json.load(f).get("price")
    else:
        old_price = None

    if new_price != old_price:
        msg = f"🚗 ราคาน้ำมันเปลี่ยน!\nเก่า: {old_price}\nใหม่: {new_price}"
        send_line(msg)

    with open(FILE_PATH, "w") as f:
        json.dump({"price": new_price}, f)

with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="check_price",
        python_callable=check_price
    )