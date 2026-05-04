#!/usr/bin/env python3
"""
统一数据更新脚本。一次性更新所有类型的数据。

用法:
    python fetch_data.py AMZN              # 更新单个个股
    python fetch_data.py AMZN AAPL MSFT    # 更新多个个股
    python fetch_data.py --all             # 更新所有个股
    python fetch_data.py --all -n 100      # 更新前100个个股
    python fetch_data.py --indices         # 更新指数（^GSPC, ^VIX）
    python fetch_data.py --sentiment       # 更新 Fear & Greed
    python fetch_data.py --everything      # 更新全部（个股+指数+情绪）

数据目录:
    data/stocks/       ← 个股
    data/indices/      ← 指数
    data/sentiment/    ← 情绪指标
"""

import json
import os
import sys
import argparse
import time
import random
import calendar
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ====== 配置 ======
DATA_ROOT = r"D:\trading\backend\data"
STOCKS_DIR = os.path.join(DATA_ROOT, "stocks")
INDICES_DIR = os.path.join(DATA_ROOT, "indices")
SENTIMENT_DIR = os.path.join(DATA_ROOT, "sentiment")

# GitHub 上的 2011-2021 早期 Fear & Greed 数据
FNG_ARCHIVE_URL = "https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear-greed.csv"
# CNN 接口（2021 至今）
FNG_CNN_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
# ==================

YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


# ============================================================
#  通用 Yahoo Finance 请求
# ============================================================
def fetch_yahoo(ticker: str, period1: int, period2: int) -> list[dict]:
    """请求 Yahoo Finance v8 API。"""
    encoded = ticker.replace("^", "%5E")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
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

        records.append({
            "date":   datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
            "open":   round(float(o), 2),
            "high":   round(float(h), 2),
            "low":    round(float(l), 2),
            "close":  round(float(c), 2),
            "volume": int(v or 0),
        })

    return records


# ============================================================
#  个股更新
# ============================================================
def update_stock(symbol: str, data_dir: str = None) -> dict:
    """增量更新单个个股/指数。"""
    if data_dir is None:
        data_dir = STOCKS_DIR

    symbol_upper = symbol.upper().strip()
    filepath = os.path.join(data_dir, f"{symbol_upper}.json")

    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    last_date = existing[-1]["date"] if existing else None

    if last_date:
        start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
    else:
        start_dt = datetime(1990, 1, 1)

    end_dt = datetime.now()

    if last_date and start_dt.date() > end_dt.date():
        return {"symbol": symbol_upper, "status": "已是最新", "last_date": last_date, "new_records": 0}

    period1 = int(start_dt.timestamp())
    period2 = int(end_dt.timestamp())

    new_records = fetch_yahoo(symbol, period1, period2)

    existing_dates = {r["date"] for r in existing}
    new_records = [r for r in new_records if r["date"] not in existing_dates]

    if not new_records:
        return {"symbol": symbol_upper, "status": "无新数据", "last_date": last_date, "new_records": 0}

    merged = existing + new_records
    merged.sort(key=lambda x: x["date"])

    os.makedirs(data_dir, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False)

    new_last = merged[-1]["date"]
    return {
        "symbol": symbol_upper,
        "status": "✅ 已更新",
        "last_date": f"{last_date} → {new_last}",
        "new_records": len(new_records),
    }


# ============================================================
#  指数更新（^GSPC, ^VIX）— 按 10 年分段
# ============================================================
def update_index(symbol: str):
    """更新指数，支持增量。"""
    filepath = os.path.join(INDICES_DIR, f"{symbol}.json")

    if os.path.exists(filepath):
        # 有历史数据，走增量
        result = update_stock(symbol, data_dir=INDICES_DIR)
        return result
    else:
        # 没有历史数据，分段全量拉取
        print(f"  📥 首次获取 {symbol}，按10年分段...")
        start_year = 1970
        now = datetime.now()
        current_year = now.year

        chunks = []
        y = start_year
        while y < current_year:
            end_y = min(y + 10, current_year + 1)
            start_dt = datetime(y, 1, 1)
            end_dt = datetime(end_y, 1, 1) if end_y <= current_year else now
            p1 = calendar.timegm(start_dt.timetuple())
            p2 = calendar.timegm(end_dt.timetuple())
            chunks.append((y, end_y - 1, p1, p2))
            y = end_y

        all_records = []
        for i, (y_start, y_end, p1, p2) in enumerate(chunks, 1):
            try:
                if i > 1:
                    time.sleep(random.uniform(1, 5))
                records = fetch_yahoo(symbol, p1, p2)
                all_records.extend(records)
                print(f"    [{i}/{len(chunks)}] {y_start}-{y_end}: {len(records)} 条")
            except Exception as e:
                print(f"    [{i}/{len(chunks)}] {y_start}-{y_end}: ❌ {e}")

        # 去重排序
        seen = set()
        unique = []
        for r in all_records:
            if r["date"] not in seen:
                seen.add(r["date"])
                unique.append(r)
        unique.sort(key=lambda x: x["date"])

        os.makedirs(INDICES_DIR, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(unique, f, ensure_ascii=False)

        return {
            "symbol": symbol,
            "status": "✅ 首次获取完成",
            "last_date": unique[-1]["date"] if unique else "N/A",
            "new_records": len(unique),
        }


# ============================================================
#  Fear & Greed 更新（GitHub 早期数据 + CNN 最新数据）
# ============================================================
def get_fng_label(value):
    if value <= 25: return "Extreme Fear"
    elif value <= 45: return "Fear"
    elif value <= 55: return "Neutral"
    elif value <= 75: return "Greed"
    else: return "Extreme Greed"


def update_fear_greed():
    """
    获取完整的 Fear & Greed 历史数据：
    1. 从 GitHub 下载 2011-至今 的合并 CSV（whit3rabbit/fear-greed-data）
    2. 从 CNN 接口补充最新数据（以防 GitHub 更新有延迟）
    3. 合并去重，保存到 data/sentiment/FEAR_GREED.json
    """
    filepath = os.path.join(SENTIMENT_DIR, "FEAR_GREED.json")
    os.makedirs(SENTIMENT_DIR, exist_ok=True)

    all_records = {}

    # ===== 第一步：从 GitHub 下载完整历史 CSV =====
    print("  📥 从 GitHub 下载历史数据 (2011-至今)...")
    try:
        resp = requests.get(FNG_ARCHIVE_URL, headers=YAHOO_HEADERS, timeout=30)
        resp.raise_for_status()

        lines = resp.text.strip().split("\n")
        header = lines[0].lower()

        # 解析 CSV 头部，找到 date 和 score 列
        cols = [c.strip() for c in header.split(",")]
        date_idx = None
        score_idx = None
        for i, col in enumerate(cols):
            if "date" in col:
                date_idx = i
            if "score" in col or "fear" in col and "greed" in col:
                score_idx = i

        if date_idx is None or score_idx is None:
            # 尝试常见格式: date,score,label
            date_idx = 0
            score_idx = 1
            print(f"    ⚠️ CSV 头: {cols}，使用默认列位置 (0=date, 1=score)")

        count = 0
        for line in lines[1:]:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) <= max(date_idx, score_idx):
                continue
            try:
                date_str = parts[date_idx].strip('"')
                # 处理不同日期格式
                if "/" in date_str:
                    dt = datetime.strptime(date_str, "%m/%d/%Y")
                elif "-" in date_str:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                else:
                    continue

                date_key = dt.strftime("%Y-%m-%d")
                value = float(parts[score_idx].strip('"'))

                if 0 <= value <= 100:
                    all_records[date_key] = {
                        "date": date_key,
                        "value": round(value, 1),
                        "label": get_fng_label(value),
                    }
                    count += 1
            except (ValueError, IndexError):
                continue

        print(f"    ✅ GitHub: {count} 条")

    except Exception as e:
        print(f"    ❌ GitHub 下载失败: {e}")

    # ===== 第二步：从 CNN 接口补充最新数据 =====
    print("  📥 从 CNN 接口补充最新数据...")
    try:
        # 从 2021 年开始拉（覆盖 GitHub 可能的延迟）
        cnn_url = f"{FNG_CNN_URL}/2021-01-01"
        resp = requests.get(cnn_url, headers=YAHOO_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        fg_data = data.get("fear_and_greed_historical", {}).get("data", [])
        cnn_count = 0

        for item in fg_data:
            ts = item.get("x", 0)
            value = item.get("y", 0)
            date_key = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")

            if 0 <= value <= 100:
                all_records[date_key] = {
                    "date": date_key,
                    "value": round(float(value), 1),
                    "label": get_fng_label(value),
                }
                cnn_count += 1

        print(f"    ✅ CNN: {cnn_count} 条")

    except Exception as e:
        print(f"    ❌ CNN 接口失败: {e}")

    # ===== 第三步：合并保存 =====
    if not all_records:
        print("  ❌ 未获取到任何 Fear & Greed 数据")
        return {"symbol": "FEAR_GREED", "status": "❌ 失败", "new_records": 0}

    records = sorted(all_records.values(), key=lambda x: x["date"])

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)

    print(f"  ✅ 合并完成: {len(records)} 条 ({records[0]['date']} ~ {records[-1]['date']})")
    print(f"  📈 最新: {records[-1]['value']} ({records[-1]['label']})")
    print(f"  💾 {filepath}")

    return {
        "symbol": "FEAR_GREED",
        "status": "✅ 已更新",
        "last_date": records[-1]["date"],
        "new_records": len(records),
    }


# ============================================================
#  主入口
# ============================================================
def get_all_stock_symbols() -> list[str]:
    return sorted([f.stem for f in Path(STOCKS_DIR).glob("*.json")])


def main():
    parser = argparse.ArgumentParser(description="统一数据更新脚本")
    parser.add_argument("symbols", nargs="*", help="个股代码，如 AMZN AAPL MSFT")
    parser.add_argument("--all", action="store_true", help="更新所有个股")
    parser.add_argument("--count", "-n", type=int, default=None, help="只更新前N个个股")
    parser.add_argument("--indices", action="store_true", help="更新指数（^GSPC, ^VIX）")
    parser.add_argument("--sentiment", action="store_true", help="更新 Fear & Greed")
    parser.add_argument("--everything", action="store_true", help="更新全部数据")
    args = parser.parse_args()

    if not any([args.symbols, args.all, args.indices, args.sentiment, args.everything]):
        parser.print_help()
        sys.exit(0)

    start_time = time.time()

    # ===== 指数 =====
    if args.indices or args.everything:
        print("\n" + "=" * 50)
        print("📊 更新指数")
        print("=" * 50)
        for idx_symbol in ["^GSPC", "^VIX"]:
            try:
                result = update_index(idx_symbol)
                print(f"  {idx_symbol:<8} {result['status']}  (+{result['new_records']} 条, {result['last_date']})")
            except Exception as e:
                print(f"  {idx_symbol:<8} ❌ 失败: {e}")
            time.sleep(random.uniform(1, 3))

    # ===== Fear & Greed =====
    if args.sentiment or args.everything:
        print("\n" + "=" * 50)
        print("💭 更新 Fear & Greed")
        print("=" * 50)
        update_fear_greed()

    # ===== 个股 =====
    if args.symbols or args.all or args.everything:
        print("\n" + "=" * 50)
        print("📈 更新个股")
        print("=" * 50)

        if args.all or args.everything:
            symbols = get_all_stock_symbols()
            if args.count:
                symbols = symbols[:args.count]
            print(f"  将更新 {len(symbols)} 个个股\n")
        else:
            symbols = [s.upper().strip() for s in args.symbols]

        success = 0
        failed = 0
        skipped = 0

        for i, sym in enumerate(symbols, 1):
            try:
                result = update_stock(sym)
                new_n = result["new_records"]
                status = result["status"]

                if new_n == 0:
                    skipped += 1
                else:
                    success += 1

                print(f"  [{i:>3}/{len(symbols)}] {sym:<6} {status}  (+{new_n} 条, {result['last_date']})")

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    wait = random.randint(30, 60)
                    print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ⏳ 限流，等待{wait}秒...")
                    time.sleep(wait)
                    try:
                        result = update_stock(sym)
                        success += 1
                        print(f"  [{i:>3}/{len(symbols)}] {sym:<6} {result['status']}  (+{result['new_records']} 条)")
                    except Exception as e2:
                        failed += 1
                        print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 重试失败: {e2}")
                else:
                    failed += 1
                    print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 失败: {e}")
            except Exception as e:
                failed += 1
                print(f"  [{i:>3}/{len(symbols)}] {sym:<6} ❌ 失败: {e}")

            if i < len(symbols):
                time.sleep(random.uniform(1, 10))

        print(f"\n  ✅ 更新: {success}  ⏭️ 跳过: {skipped}  ❌ 失败: {failed}  共: {len(symbols)}")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 50}")
    print(f"⏱️ 总耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()