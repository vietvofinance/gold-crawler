import requests
import os
from datetime import datetime
from supabase import create_client, Client

# Lấy biến môi trường từ GitHub Actions hoặc local .env
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def generate_id(prefix):
    now = datetime.now()
    return f"{prefix}{now.strftime('%y%m%d%H')}"

def parse_price(data):
    try:
        return int(data) if data and data != "0" else None
    except:
        return None

def parse_datetime_safe(s):
    try:
        return datetime.strptime(s, "%d/%m/%Y %H:%M")
    except:
        return None

def crawl():
    print("🔍 Crawling from BTMC API...")
    url = "http://api.btmc.vn/api/BTMCAPI/getpricebtmc?key=3kd8ub1llcg9t45hnoh8hmn7t5kc2v"
    try:
        res = requests.get(url, timeout=10)
        data = res.json()
    except Exception as e:
        print(f"❌ Request or JSON decode failed: {e}")
        print("Raw response:", res.text[:300])
        return []

    data_list = data.get("DataList", {}).get("Data", [])

    # Tìm timestamp mới nhất
    all_timestamps = []
    for item in data_list:
        for i in range(1, 10):
            ts = parse_datetime_safe(item.get(f"@d_{i}"))
            if ts:
                all_timestamps.append(ts)

    if not all_timestamps:
        print("⚠️ Không tìm thấy timestamp nào hợp lệ.")
        return []

    latest_ts = max(all_timestamps)
    print(f"📌 Latest timestamp found in API: {latest_ts}")

    # Lọc các dòng có timestamp == latest
    rows = []
    for item in data_list:
        for i in range(1, 10):
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
            timestamp = parse_datetime_safe(time_str)

            if not timestamp or timestamp != latest_ts:
                continue

            row = {
                "id": generate_id(prefix),
                "type": prefix.replace("G", ""),
                "buy_price": buy,
                "sell_price": sell,
                "unit": "10 chỉ",
                "source": "btmc.vn",
                "timestamp": timestamp.isoformat()
            }
            rows.append(row)

    print(f"✅ Parsed {len(rows)} rows (latest only).")
    return rows

def insert_data(rows):
    if not rows:
        print("⚠️ No data to insert.")
        return

    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        for row in rows:
            try:
                res = supabase.table("gold_price").insert(row, upsert=False).execute()
                print(f"✅ Inserted: {row['id']}")
            except Exception as insert_error:
                print(f"❌ Insert error for {row['id']}: {insert_error}")
    except Exception as e:
        print(f"❌ Supabase connection/config error: {e}")

if __name__ == "__main__":
    data = crawl()
    print(f"🐍 Số dòng crawl được: {len(data)}")
    for row in data:
        print("➡️", row)
    insert_data(data)
