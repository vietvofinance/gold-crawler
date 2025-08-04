import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import psycopg2

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
    return int(data) if data and data != '0' else None

def crawl():
    print("🔍 Crawling from BTMC API...")
    url = "http://api.btmc.vn/api/BTMCAPI/getpricebtmc?key=3kd8ub1llcg9t45hnoh8hmn7t5kc2v"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            print(f"❌ HTTP Error: {res.status_code}")
            print("Response:", res.text[:300])
            return []
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return []

    try:
        root = ET.fromstring(res.text)
    except ET.ParseError as e:
        print("❌ XML Parse Error:", e)
        print("Raw response:", res.text[:300])
        return []

    rows = []
    for child in root.findall("Data"):
        attrs = child.attrib
        for k, v in attrs.items():
            if "n_" in k:
                name = v.upper()
                if "SJC" in name:
                    prefix = "GSJC"
                elif "BTMC" in name:
                    prefix = "GBTMC"
                elif "NGUYÊN LIỆU" in name:
                    prefix = "GVNL"
                else:
                    continue

                buy = parse_price(attrs.get(f"pb_{k[-1]}"))
                sell = parse_price(attrs.get(f"ps_{k[-1]}"))
                time_str = attrs.get(f"d_{k[-1]}")
                timestamp = datetime.strptime(time_str, "%d/%m/%Y %H:%M")

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
    insert_data(data)
