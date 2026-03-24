from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import requests
from bs4 import BeautifulSoup

FILE_PATH = "/tmp/last_prices.json"

TARGETS = {
    "แก๊สโซฮอล์ 95": "Gasohol 95",
    "แก๊สโซฮอล์ 91": "Gasohol 91",
    "แก๊สโซฮอล์ E20": "Gasohol E20",
    "ดีเซล": "Diesel",
}


def scrape_prices():
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

    found = {}

    for i, line in enumerate(ptt_lines):
        if line in TARGETS and i + 1 < len(ptt_lines):
            next_line = ptt_lines[i + 1]
            try:
                found[line] = float(next_line)
            except ValueError:
                pass

    return found


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


def load_old_prices():
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_new_prices(prices):
    with open(FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(prices, f, ensure_ascii=False)


def format_line(th_name, en_name, old_price, new_price):
    if new_price is None:
        return f"• {th_name} ({en_name}): ไม่พบราคา"

    if old_price is None:
        return f"• {th_name} ({en_name}): {new_price:.2f} บาท (รอบแรก)"

    diff = round(new_price - old_price, 2)

    if diff > 0:
        return f"• {th_name} ({en_name}): {new_price:.2f} บาท ⬆️ +{diff:.2f}"
    elif diff < 0:
        return f"• {th_name} ({en_name}): {new_price:.2f} บาท ⬇️ {diff:.2f}"
    else:
        return f"• {th_name} ({en_name}): {new_price:.2f} บาท ➖ 0.00"


def check_prices():
    new_prices = scrape_prices()
    old_prices = load_old_prices()

    lines = []
    changed = False

    for th_name, en_name in TARGETS.items():
        old_price = old_prices.get(th_name)
        new_price = new_prices.get(th_name)

        lines.append(format_line(th_name, en_name, old_price, new_price))

        if old_price is None and new_price is not None:
            changed = True
        elif old_price is not None and new_price is not None and round(new_price - old_price, 2) != 0:
            changed = True

    if changed:
        msg = "🚗 ราคาน้ำมัน ปตท. อัปเดต\n\n" + "\n".join(lines)
        send_line(msg)

    save_new_prices(new_prices)


with DAG(
    dag_id="oil_price_alert",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="check_price",
        python_callable=check_prices
    )