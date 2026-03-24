from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import requests
from bs4 import BeautifulSoup

FILE_PATH = "/tmp/last_prices_multi_brand.json"

BRANDS = {
    "ปตท.": "ราคานํ้ามัน ปตท. (ptt)",
    "บางจาก": "ราคานํ้ามันบางจาก (bcp)",
    "เชลล์": "ราคานํ้ามันเชลล์ (shell)",
    "พีที": "ราคานํ้ามันพีที (pt)",
    "คาลเท็กซ์": "ราคานํ้ามันคาลเท็กซ์ (caltex)",
}

TARGETS = [
    "แก๊สโซฮอล์ 95",
    "แก๊สโซฮอล์ 91",
    "แก๊สโซฮอล์ E20",
    "ดีเซล",
]

FUEL_ICON = {
    "แก๊สโซฮอล์ 95": "🟠",
    "แก๊สโซฮอล์ 91": "🟢",
    "แก๊สโซฮอล์ E20": "🟢",
    "ดีเซล": "🔵",
}


def normalize_line(line: str) -> str:
    return line.replace("ราคาน้ำมัน", "ราคานํ้ามัน").strip()


def scrape_prices():
    url = "https://gasprice.kapook.com/gasprice.php"
    headers = {"User-Agent": "Mozilla/5.0"}

    res = requests.get(url, headers=headers, timeout=30)
    res.raise_for_status()
    res.encoding = res.apparent_encoding

    text = BeautifulSoup(res.text, "html.parser").get_text("\n", strip=True)
    lines = [normalize_line(line) for line in text.splitlines() if line.strip()]

    results = {}
    brand_items = list(BRANDS.items())

    for idx, (brand_name, brand_header) in enumerate(brand_items):
        start_idx = None
        end_idx = None

        for i, line in enumerate(lines):
            if line == normalize_line(brand_header):
                start_idx = i
                break

        if start_idx is None:
            results[brand_name] = {}
            continue

        next_headers = [normalize_line(v) for _, v in brand_items[idx + 1:]]

        for i in range(start_idx + 1, len(lines)):
            if lines[i] in next_headers:
                end_idx = i
                break

        if end_idx is None:
            end_idx = len(lines)

        brand_lines = lines[start_idx:end_idx]
        found = {}

        for i, line in enumerate(brand_lines):
            if line in TARGETS and i + 1 < len(brand_lines):
                try:
                    found[line] = float(brand_lines[i + 1])
                except ValueError:
                    pass

        results[brand_name] = found

    return results


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


def diff_text(old_price, new_price):
    if new_price is None:
        return "ไม่พบราคา"

    if old_price is None:
        return "🆕 รอบแรก"

    diff = round(new_price - old_price, 2)

    if diff > 0:
        return f"📈 +{diff:.2f}"
    elif diff < 0:
        return f"📉 {diff:.2f}"
    else:
        return "➖ คงเดิม"


def has_change(old_prices, new_prices):
    for brand in BRANDS:
        old_brand = old_prices.get(brand, {})
        new_brand = new_prices.get(brand, {})

        for fuel in TARGETS:
            old_price = old_brand.get(fuel)
            new_price = new_brand.get(fuel)

            if old_price is None and new_price is not None:
                return True

            if old_price is not None and new_price is not None:
                if round(new_price - old_price, 2) != 0:
                    return True

    return False


def build_message(old_prices, new_prices):
    lines = ["⛽ อัปเดตราคาน้ำมัน", ""]

    first_run = True
    for brand in BRANDS:
        for fuel in TARGETS:
            if old_prices.get(brand, {}).get(fuel) is not None:
                first_run = False
                break
        if not first_run:
            break

    for brand in BRANDS:
        lines.append(f"🏷️ {brand}")
        brand_old = old_prices.get(brand, {})
        brand_new = new_prices.get(brand, {})

        for fuel in TARGETS:
            icon = FUEL_ICON.get(fuel, "•")
            old_price = brand_old.get(fuel)
            new_price = brand_new.get(fuel)

            if new_price is None:
                lines.append(f"{icon} {fuel}  ไม่พบราคา")
            else:
                change = diff_text(old_price, new_price)
                lines.append(f"{icon} {fuel}  {new_price:.2f} บาท  {change}")

        lines.append("")

    if first_run:
        lines.append("📝 หมายเหตุ: รอบแรกของการติดตามราคา")

    return "\n".join(lines).strip()


def check_prices():
    new_prices = scrape_prices()
    old_prices = load_old_prices()

    if has_change(old_prices, new_prices):
        msg = build_message(old_prices, new_prices)
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