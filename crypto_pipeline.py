import requests
import boto3
import json
import psycopg2
from datetime import datetime

BUCKET = "data-engineering-sero-2026"

# ── EXTRACT ──────────────────────────────────────────
def extract():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,binancecoin",
        "vs_currencies": "usd",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params)
    data = response.json()
    data["timestamp"] = datetime.utcnow().isoformat()
    print(f"Extracted: {data}")
    return data

# ── UPLOAD RAW TO S3 ─────────────────────────────────
def upload_raw(data):
    s3 = boto3.client("s3")
    key = f"raw/crypto_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data)
    )
    print(f"Uploaded raw to s3://{BUCKET}/{key}")
    return key

# ── TRANSFORM ────────────────────────────────────────
def transform(data):
    timestamp = data["timestamp"]
    rows = []
    for coin, values in data.items():
        if coin == "timestamp":
            continue
        rows.append({
            "coin": coin,
            "price_usd": values["usd"],
            "change_24h": values["usd_24h_change"],
            "timestamp": timestamp
        })
    print(f"Transformed {len(rows)} rows")
    return rows

# ── LOAD TO POSTGRESQL ───────────────────────────────
def load(rows):
    conn = psycopg2.connect(
        host="localhost",
        dbname="company",
        user="postgres",
        password="5157776",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id        SERIAL PRIMARY KEY,
            coin      VARCHAR(50),
            price_usd NUMERIC,
            change_24h NUMERIC,
            timestamp VARCHAR(50)
        )
    """)
    for row in rows:
        cur.execute("""
            INSERT INTO crypto_prices (coin, price_usd, change_24h, timestamp)
            VALUES (%s, %s, %s, %s)
        """, (row["coin"], row["price_usd"], row["change_24h"], row["timestamp"]))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into PostgreSQL")

# ── RUN PIPELINE ─────────────────────────────────────
raw_data = extract()
upload_raw(raw_data)
rows = transform(raw_data)
load(rows)
print("Pipeline complete.")