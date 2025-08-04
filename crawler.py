import requests
import json
import os
import psycopg2
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")

def generate_id(prefix):
    now = datetime.now()
    return f"{prefix}{now.strftime('%y%m%d%H')}"

def parse_price(data):
    try:
        return int(data) if data and data != "0" else None
    except:
        return None

def crawl():
    print("🔍 Crawling from BTMC API...")
    url = "http://api.btmc.vn/api/BTMCAPI/getpricebtmc?key=3kd8ub1llcg9t45hnoh8hmn7t5kc2v"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()  # Sửa ở đây: dùng JSON parser
    except Exception as e:
        print(f"❌ Request or JSON decode failed: {e}")
        print("Raw response:", res.text[:300])
        return []

    rows = []
    data_list = data.get("DataList", {}).get("Data", [])

    for item in data_list:
        attrs = item["@row"]
        for i in range(1, 10):  # Kiểm tra đến 9 cặp là đủ
            name = item.get(f"@n_{i}")
            if not name:
                continue
            name_upper = name.upper()

            if "SJC" in name_upper:
                prefix = "GSJC"
            elif "BTMC" in name_upper:
                prefix = "GBTMC"
            elif "NGUYÊN LIỆU" in name_upper:
                prefix = "GVNL"
            else:
                continue

            buy = parse_price(item.get(f"@pb_{i}"))
            sell = parse_price(item.get(f"@ps_{i}"))
            time_str = item.get(f"@d_{i}")
            try:
                timestamp = datetime.strptime(time_str, "%d/%m/%Y %H:%M")
            except:
                timestamp = datetime.now()

            row = {
                "id": generate_id(prefix),
                "type": prefix.replace("G", ""),
                "buy_price": buy,
                "sell_price": sell,
                "unit": "1 lượng",
                "source": "btmc.vn",
                "timestamp": timestamp
            }
            rows.append(row)

    print(f"✅ Parsed {len(rows)} rows.")
    return rows

def insert_data(rows):
    if not rows:
        print("⚠️ No data to insert.")
        return

    try:
        conn = psycopg2.connect(
            dbname=SUPABASE_DB,
            user=SUPABASE_USER,
            password=SUPABASE_PASSWORD,
            host=SUPABASE_HOST,
            port=SUPABASE_PORT
        )
        cur = conn.cursor()
        for row in rows:
            cur.execute("""
                INSERT INTO gold_price (id, type, buy_price, sell_price, unit, source, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                row["id"], row["type"], row["buy_price"], row["sell_price"],
                row["unit"], row["source"], row["timestamp"]
            ))
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Inserted data into Supabase.")
    except Exception as e:
        print(f"❌ Database insert error: {e}")

if __name__ == "__main__":
    data = crawl()
    print(f"🐍 Số dòng crawl được: {len(data)}")
    for row in data:
        print("➡️ ", row)
    insert_data(data)
