import os
import json
import boto3
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from uuid import uuid4

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = os.getenv("RAW_BUCKET")
    raw_prefix = os.getenv("RAW_PREFIX")
    base_url = os.getenv("BASE_URL")
    api_key = os.getenv("CG_API_KEY")
    vs_currency = os.getenv("VS_CURRENCY")
    symbols_csv = os.getenv("SYMBOLS")

    params = {
        "vs_currency": vs_currency,
        "ids": symbols_csv,
        "order": "market_cap_desc",
        "per_page": "250",
        "page": "1",
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d"
    }

    url = base_url + "?" + urllib.parse.urlencode(params)

    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key
    }

    req = urllib.request.Request(url, headers=headers, method="GET")

    with urllib.request.urlopen(req, timeout=30) as resp:
        status = resp.status
        raw_body = resp.read().decode("utf-8")
        data = json.loads(raw_body)

    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    ingest_hour = now.strftime("%H")
    ts = now.strftime("%Y%m%d_%H%M%S")
    run_id = uuid4().hex[:8]

    s3_key = f"{raw_prefix}/ingest_date={ingest_date}/ingest_hour={ingest_hour}/markets_{ts}_{run_id}.json"

    payload = {
        "source": "coingecko",
        "base_url": base_url,
        "endpoint": "coins/markets",
        "ingest_ts_utc": now.isoformat(),
        "vs_currency": vs_currency,
        "requested_ids": symbols_csv,
        "http_status": status,
        "item_count": len(data) if isinstance(data, list) else None,
        "data": data
    }

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json"
    )
    
    return {
        "status": "ok",
        "bucket": bucket,
        "key": s3_key,
        "coins_requested": len(symbols_csv.split(",")),
        "items_returned": len(data) if isinstance(data, list) else None
    }