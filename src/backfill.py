import os
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv
from src.transform import to_silver_df
from src.load import to_gold_brl_df

load_dotenv()

RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")
BASE = "USD"

def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def fetch_history_day(api_key: str, day_str: str) -> dict:
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/history/{BASE}/{day_str}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "conversion_rates" not in data and "rates" in data:
        data["conversion_rates"] = data["rates"]
    if "base_code" not in data:
        data["base_code"] = BASE
    if "time_last_update_unix" not in data:
        dt = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        data["time_last_update_unix"] = int(dt.timestamp())
    return data

def backfill(start_str: str, end_str: str):
    api_key = os.getenv("EXCHANGERATE_API_KEY")
    if not api_key:
        raise RuntimeError("EXCHANGERATE_API_KEY ausente no .env")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    d0 = datetime.strptime(start_str, "%Y-%m-%d")
    d1 = datetime.strptime(end_str, "%Y-%m-%d")
    for d in daterange(d0, d1):
        day = d.strftime("%Y-%m-%d")
        data = fetch_history_day(api_key, day)
        raw_path = RAW_DIR / f"{day}.json"
        pd.Series(data).to_json(raw_path, force_ascii=False, indent=2)
        df_silver = to_silver_df(data)
        silver_path = SILVER_DIR / f"{day}.parquet"
        df_silver.to_parquet(silver_path, index=False)
        df_gold = to_gold_brl_df(df_silver)
        gold_path = GOLD_DIR / f"exchange_rates_brl_base_{day}.parquet"
        df_gold.to_parquet(gold_path, index=False)
        md_path = GOLD_DIR / f"daily_summary_{day}.md"
        if not md_path.exists():
            md_path.write_text(f"Resumo Cambial - {d.strftime('%d/%m/%Y')}\n\n(Gere com `python -m src.cli enrich` para o dia atual)", encoding="utf-8")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = p.parse_args()
    backfill(args.start, args.end)

if __name__ == "__main__":
    main()
