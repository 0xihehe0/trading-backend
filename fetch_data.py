#!/usr/bin/env python3
"""
批量增量更新标普500历史数据（与 newstock.py 使用同一套 Yahoo Finance v8 API）。

用法:
    python fetch_data.py AMZN              # 更新单个
    python fetch_data.py AMZN AAPL MSFT    # 更新多个
    python fetch_data.py --all             # 更新 sp500_split/ 下所有公司

无需 yfinance，只依赖 requests（Flask 项目一般已装好）。
"""

import json
import os
import sys
import argparse
import time
import random
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ====== 配置 ======
DATA_DIR = r"D:\trading\backend\data\stocks"
# ==================

YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def fetch_yahoo(ticker: str, period1: int, period2: int) -> list[dict]:
    """
    请求 Yahoo Finance v8 API，返回标准格式记录列表。
    与 newstock.py 中的 _fetch_yahoo 逻辑完全一致。
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={period1}&period2={period2}&interval=1d"
    )

    resp = requests.get(url, headers=YAHOO_HEADERS, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    result = data.get("chart", {}).get("result", [])
    if not result:
        return []

    r = result[0]
    timestamps = r.get("timestamp") or []
    quote = (r.get("indicators", {}).get("quote") or [{}])[0]

    records = []
    for i, ts in enumerate(timestamps):
        o = quote.get("open", [None])[i]
        h = quote.get("high", [None])[i]
        l = quote.get("low", [None])[i]
        c = quote.get("close", [None])[i]
        v = quote.get("volume", [0])[i]

        if o is None or c is None:
            continue

        date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        records.append({
            "date":   date_str,
            "open":   round(float(o), 2),
            "high":   round(float(h), 2),
            "low":    round(float(l), 2),
            "close":  round(float(c), 2),
            "volume": int(v or 0),
        })

    return records


def update_symbol(symbol: str) -> dict:
    """更新单个公司的数据。"""
    symbol = symbol.upper().strip()
    filepath = os.path.join(DATA_DIR, f"{symbol}.json")

    # 读取现有数据
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    last_date = existing[-1]["date"] if existing else None

    # 计算拉取范围
    if last_date:
        start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    else:
        start_dt = datetime(1990, 1, 1)

    end_dt = datetime.now()

    # 已是最新
    if last_date and start_dt.date() > end_dt.date():
        return {"symbol": symbol, "status": "已是最新", "last_date": last_date, "new_records": 0}

    period1 = int(start_dt.timestamp())
    period2 = int(end_dt.timestamp())

    # 请求 Yahoo
    new_records = fetch_yahoo(symbol, period1, period2)

    # 去重
    existing_dates = {r["date"] for r in existing}
    new_records = [r for r in new_records if r["date"] not in existing_dates]

    if not new_records:
        return {"symbol": symbol, "status": "无新数据", "last_date": last_date, "new_records": 0}

    # 合并、排序、写入
    merged = existing + new_records
    merged.sort(key=lambda x: x["date"])

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)

    new_last = merged[-1]["date"]
    return {
        "symbol": symbol,
        "status": "✅ 已更新",
        "last_date": f"{last_date} → {new_last}",
        "new_records": len(new_records),
    }


def get_all_symbols() -> list[str]:
    """读取 sp500_split/ 下所有 JSON 文件名。"""
    return sorted([f.stem for f in Path(DATA_DIR).glob("*.json")])


def main():
    parser = argparse.ArgumentParser(description="批量增量更新标普500历史数据")
    parser.add_argument("symbols", nargs="*", help="股票代码，如 AMZN AAPL MSFT")
    parser.add_argument("--all", action="store_true", help="更新所有公司")
    parser.add_argument("--count", "-n", type=int, default=None, help="只更新前N个（配合 --all 使用）")
    args = parser.parse_args()

    if not os.path.isdir(DATA_DIR):
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        sys.exit(1)

    if args.all:
        symbols = get_all_symbols()
        if args.count:
            symbols = symbols[:args.count]
        print(f"📋 将更新 {len(symbols)} 个公司\n")
    elif args.symbols:
        symbols = [s.upper().strip() for s in args.symbols]
    else:
        parser.print_help()
        sys.exit(0)

    success = 0
    failed = 0
    skipped = 0
    start_time = time.time()

    for i, sym in enumerate(symbols, 1):
        try:
            result = update_symbol(sym)
            new_n = result["new_records"]
            last = result["last_date"]
            status = result["status"]

            if new_n == 0:
                skipped += 1
            else:
                success += 1

            print(f"  [{i:>3}/{len(symbols)}] {sym:<6} {status}  (+{new_n} 条, {last})")

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = random.randint(30, 60)
                print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ⏳ 被限流，等待{wait}秒后重试...")
                time.sleep(wait)
                try:
                    result = update_symbol(sym)
                    new_n = result["new_records"]
                    last = result["last_date"]
                    status = result["status"]
                    success += 1
                    print(f"  [{i:>3}/{len(symbols)}] {sym:<6} {status}  (+{new_n} 条, {last})")
                except Exception as e2:
                    failed += 1
                    print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 重试失败: {e2}")
            else:
                failed += 1
                print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 失败: {e}")

        except Exception as e:
            failed += 1
            print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 失败: {e}")

        # 随机间隔 1~10 秒，模拟真人操作节奏
        if i < len(symbols):
            delay = random.uniform(1, 10)
            time.sleep(delay)

    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"✅ 更新: {success}  ⏭️ 跳过: {skipped}  ❌ 失败: {failed}  共: {len(symbols)}")
    print(f"⏱️ 实际耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()