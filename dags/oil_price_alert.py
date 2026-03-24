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
    url = "https://gasprice.kapook.com/gasprice.php"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()

    text = BeautifulSoup(res.text, "html.parser").get_text(" ", strip=True)

    # เอาเฉพาะช่วงของ ปตท.
    section_match = re.search(
        r"ราคานํ้ามัน ปตท\. \(ptt\)(.*?)(ราคานํ้ามันบางจาก \(bcp\)|$)",
        text,
        re.DOTALL,
    )
    if not section_match:
        raise ValueError("ไม่เจอ section ปตท. ในหน้า Kapook")

    ptt_section = section_match.group(1)

    # หาแก๊สโซฮอล์ 95
    price_match = re.search(r"แก๊สโซฮอล์ 95\s+(\d+\.\d+)", ptt_section)
    if not price_match:
        raise ValueError("ไม่เจอราคาแก๊สโซฮอล์ 95 ของ ปตท. ในหน้า Kapook")

    return price_match.group(1)


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
        msg = f"🚗 ราคาน้ำมัน ปตท. (แก๊สโซฮอล์ 95) เปลี่ยน!\nเก่า: {old_price}\nใหม่: {new_price}"
        send_line(msg)

    with open(FILE_PATH, "w", encoding="utf-8") as f:
        json.dump({"price": new_price}, f, ensure_ascii=False)


with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="check_price",
        python_callable=check_price
    )