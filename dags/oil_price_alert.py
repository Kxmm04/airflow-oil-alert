from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import requests
from bs4 import BeautifulSoup

FILE_PATH = "/tmp/last_price.json"


def scrape_price():
    url = "https://gasprice.kapook.com/gasprice.php"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()
    res.encoding = res.apparent_encoding

    text = BeautifulSoup(res.text, "html.parser").get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    # หา section ของ ปตท.
    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if "ราคานํ้ามัน ปตท. (ptt)" in line or "ราคาน้ำมัน ปตท. (ptt)" in line:
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("ไม่เจอ section ปตท.")

    for i in range(start_idx + 1, len(lines)):
        if "ราคานํ้ามันบางจาก (bcp)" in lines[i] or "ราคาน้ำมันบางจาก (bcp)" in lines[i]:
            end_idx = i
            break

    if end_idx is None:
        end_idx = len(lines)

    ptt_lines = lines[start_idx:end_idx]

    # หา "แก๊สโซฮอล์ 95" แล้วเอาบรรทัดถัดไปเป็นราคา
    for i, line in enumerate(ptt_lines):
        if line == "แก๊สโซฮอล์ 95":
            if i + 1 < len(ptt_lines):
                price = ptt_lines[i + 1]
                # เช็กว่าหน้าตาเหมือนราคา
                try:
                    float(price)
                    return price
                except ValueError:
                    raise ValueError(f'เจอ "แก๊สโซฮอล์ 95" แต่บรรทัดถัดไปไม่ใช่ราคา: {price}')

    raise ValueError("ไม่เจอราคาแก๊สโซฮอล์ 95 ของ ปตท.")


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