#!/usr/bin/env python3
"""
增量更新标普500公司历史数据。

用法:
    python fetch_data.py AMZN            # 更新单个公司
    python fetch_data.py AMZN AAPL MSFT  # 更新多个公司
    python fetch_data.py --all           # 更新 sp500_split/ 下所有公司

逻辑:
    1. 读取 sp500_split/{SYMBOL}.json，找到最后一条记录的日期
    2. 用 yfinance 拉取 (最后日期+1天) ~ 今天 的数据
    3. 追加到原文件，保持格式统一: {date, open, high, low, close, volume}

依赖:
    pip install yfinance
"""

import json
import os
import sys
import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("📦 安装 yfinance...")
    os.system(f"{sys.executable} -m pip install yfinance -q")
    import yfinance as yf


# ====== 配置 ======
DATA_DIR = r"D:\trading\backend\data\sp500_split"
# ==================


def get_last_date(filepath: str) -> str | None:
    """读取 JSON 文件，返回最后一条记录的日期字符串。"""
    if not os.path.exists(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        records = json.load(f)
    if not records:
        return None
    return records[-1].get("date")


def fetch_new_data(symbol: str, start_date: str) -> list[dict]:
    """
    用 yfinance 拉取从 start_date 到今天的日线数据。
    返回格式与本地 JSON 一致: [{date, open, high, low, close, volume}, ...]
    """
    ticker = yf.Ticker(symbol)
    # start 是开区间起点的下一天，end 不需要指定（默认到最新）
    df = ticker.history(start=start_date, auto_adjust=True)

    if df.empty:
        return []

    records = []
    for date_idx, row in df.iterrows():
        records.append({
            "date":   date_idx.strftime("%Y-%m-%d"),
            "open":   round(float(row["Open"]), 2),
            "high":   round(float(row["High"]), 2),
            "low":    round(float(row["Low"]), 2),
            "close":  round(float(row["Close"]), 2),
            "volume": int(row["Volume"]),
        })

    return records


def update_symbol(symbol: str) -> dict:
    """
    更新单个公司的数据，返回结果摘要。
    """
    symbol = symbol.upper().strip()
    filepath = os.path.join(DATA_DIR, f"{symbol}.json")

    # 读取现有数据
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    last_date = existing[-1]["date"] if existing else None

    if last_date:
        # 从最后日期的下一天开始拉
        start = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # 没有历史数据，拉全部（yfinance 默认拉 max）
        start = "1960-01-01"

    # 检查是否已经是最新
    today = datetime.now().strftime("%Y-%m-%d")
    if last_date and last_date >= today:
        return {"symbol": symbol, "status": "已是最新", "last_date": last_date, "new_records": 0}

    # 拉取新数据
    new_records = fetch_new_data(symbol, start)

    if not new_records:
        return {"symbol": symbol, "status": "无新数据", "last_date": last_date, "new_records": 0}

    # 去重：确保不会重复追加（以 date 为 key）
    existing_dates = {r["date"] for r in existing}
    new_records = [r for r in new_records if r["date"] not in existing_dates]

    if not new_records:
        return {"symbol": symbol, "status": "无新数据", "last_date": last_date, "new_records": 0}

    # 追加并写入
    merged = existing + new_records
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
    """读取 sp500_split/ 下所有 JSON 文件名作为股票代码列表。"""
    files = Path(DATA_DIR).glob("*.json")
    return sorted([f.stem for f in files])


def main():
    parser = argparse.ArgumentParser(description="增量更新标普500公司历史数据")
    parser.add_argument("symbols", nargs="*", help="股票代码，如 AMZN AAPL MSFT")
    parser.add_argument("--all", action="store_true", help="更新所有公司")
    args = parser.parse_args()

    if not os.path.isdir(DATA_DIR):
        print(f"❌ 数据目录不存在: {DATA_DIR}")
        sys.exit(1)

    if args.all:
        symbols = get_all_symbols()
        print(f"📋 将更新全部 {len(symbols)} 个公司\n")
    elif args.symbols:
        symbols = [s.upper().strip() for s in args.symbols]
    else:
        parser.print_help()
        sys.exit(0)

    success = 0
    failed = 0

    for i, sym in enumerate(symbols, 1):
        try:
            result = update_symbol(sym)
            status = result["status"]
            new_n = result["new_records"]
            last = result["last_date"]
            print(f"  [{i:>3}/{len(symbols)}] {sym:<6} {status}  (新增 {new_n} 条, 日期: {last})")
            success += 1
        except Exception as e:
            print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 失败: {e}")
            failed += 1

        # 批量更新时稍微限速，避免被 Yahoo 封
        if len(symbols) > 5 and i % 10 == 0:
            time.sleep(1)

    print(f"\n{'='*50}")
    print(f"✅ 成功: {success}  ❌ 失败: {failed}  共: {len(symbols)}")


if __name__ == "__main__":
    main()