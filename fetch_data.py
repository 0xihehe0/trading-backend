import os
import json
import time
import random
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

# ========== 配置 ==========
CSV_PATH = "data/sp500_symbols.csv"
PRICE_JSON_PATH = "data/sp500_prices.json"
OUTPUT_DIR = os.path.dirname(PRICE_JSON_PATH)

# yfinance 下载参数
YFINANCE_THREADS = False         # 关闭内部并发，降低限流
AUTO_ADJUST = True               # 使用复权 OHLC
INTERVAL = "1d"

# 批量下载的分片大小
CHUNK_SIZE = 100

# 指数退避
BACKOFF_INITIAL = 2
BACKOFF_MAX = 60

# 常规睡眠（防抖动）
SLEEP_BASE = 2
SLEEP_JITTER = 3


# ========== 工具函数 ==========
def ensure_directory_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


def safe_sleep(base=SLEEP_BASE, jitter=SLEEP_JITTER):
    time.sleep(base + random.random() * jitter)


def _to_float(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
        return round(float(x), 2)
    except Exception:
        return None


def _to_int(x):
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def atomic_write_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def load_symbols():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Symbol list CSV not found: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    return (
        df["Symbol"]
        .dropna()
        .astype(str)
        .str.replace(".", "-", regex=False)
        .str.strip()
        .unique()
        .tolist()
    )


def load_price_data():
    if os.path.exists(PRICE_JSON_PATH):
        with open(PRICE_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_price_data(price_data):
    atomic_write_json(PRICE_JSON_PATH, price_data)


def parse_download_frame(df_all: pd.DataFrame, symbols: list) -> dict:
    """
    将 yf.download 返回的 DataFrame 拆分成 {sym: DataFrame}。
    同时兼容单 ticker（无多级列）与多 ticker（多级列）两种情况。
    """
    out = {}
    if df_all.empty:
        return out

    if isinstance(df_all.columns, pd.MultiIndex):
        # 多 ticker: 顶层是 symbol
        level0 = df_all.columns.get_level_values(0)
        for sym in symbols:
            if sym in level0:
                sub = df_all[sym].dropna(subset=["Close"], how="any").reset_index()
                out[sym] = sub
    else:
        # 单 ticker: 直接就是这个符号的表
        sub = df_all.dropna(subset=["Close"], how="any").reset_index()
        # 可能 symbols 里只有一个；若不止一个，这里无法区分（但我们分批时会控制只给一个）
        out[symbols[0]] = sub

    return out


def build_entries(df_sym, last_date):
    entries = []
    for _, row in df_sym.iterrows():
        d = row["Date"].date()
        if (not last_date) or (d > last_date):
            entries.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "open": _to_float(row.get("Open")),
                    "high": _to_float(row.get("High")),
                    "low": _to_float(row.get("Low")),
                    "close": _to_float(row.get("Close")),
                    "volume": _to_int(row.get("Volume")),
                }
            )
    return entries


def clean_and_sort(entries: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for e in entries:
        dt = e["date"]
        if dt not in seen:
            seen.add(dt)
            out.append(e)
    out.sort(key=lambda x: x["date"])
    return out


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def download_with_backoff(**kwargs) -> pd.DataFrame:
    wait = BACKOFF_INITIAL
    while True:
        try:
            return yf.download(**kwargs)
        except YFRateLimitError:
            print(f"⚠️ 批量限流，等待 {wait}s 后重试…")
            time.sleep(wait)
            wait = min(wait * 2, BACKOFF_MAX)
        except Exception as e:
            print(f"❌ 批量下载异常：{e}")
            # 返回空 DF 让上层降级处理
            return pd.DataFrame()


def fetch_one_with_backoff(symbol, start_date=None, end_date=None) -> pd.DataFrame:
    wait = BACKOFF_INITIAL
    while True:
        try:
            if start_date:
                return yf.Ticker(symbol).history(
                    start=start_date.strftime("%Y-%m-%d"),
                    end=end_date.strftime("%Y-%m-%d"),
                    interval=INTERVAL,
                    auto_adjust=AUTO_ADJUST,
                )
            else:
                return yf.Ticker(symbol).history(
                    period="max", interval=INTERVAL, auto_adjust=AUTO_ADJUST
                )
        except YFRateLimitError:
            print(f"[{symbol}] 限流，等待 {wait}s 再试…")
            time.sleep(wait)
            wait = min(wait * 2, BACKOFF_MAX)
        except Exception as e:
            print(f"[{symbol}] 单支下载异常：{e}")
            return pd.DataFrame()


# ========== 主流程 ==========
def fetch_and_update_prices():
    ensure_directory_exists(OUTPUT_DIR)
    symbols = load_symbols()
    price_data = load_price_data()
    today = datetime.now().date()

    # 分组：需要全量 vs 需要增量
    need_full, need_incr = [], []
    earliest_incr = None

    for sym in symbols:
        history = price_data.get(sym, [])
        if history:
            try:
                last_date = datetime.strptime(history[-1]["date"], "%Y-%m-%d").date()
            except Exception:
                last_date = None
        else:
            last_date = None

        if last_date:
            need_incr.append((sym, last_date))
            start_candidate = last_date + timedelta(days=1)
            if earliest_incr is None or start_candidate < earliest_incr:
                earliest_incr = start_candidate
        else:
            need_full.append(sym)

    total = len(need_full) + len(need_incr)
    if total == 0:
        print("✅ 所有符号均已最新。")
        return price_data

    print(f"🧩 需要全量: {len(need_full)}，需要增量: {len(need_incr)}")

    # 1) 先处理增量（按最早增量日期一起批量拉）
    if need_incr:
        syms = [s for s, _ in need_incr]
        for batch in chunked(syms, CHUNK_SIZE):
            print(f"🔀 批量增量 {len(batch)} 支，从 {earliest_incr} 到 {today}")
            df_all = download_with_backoff(
                tickers=batch,
                start=earliest_incr.strftime("%Y-%m-%d"),
                end=today.strftime("%Y-%m-%d"),
                interval=INTERVAL,
                group_by="ticker",
                threads=YFINANCE_THREADS,
                auto_adjust=AUTO_ADJUST,
            )
            if df_all.empty:
                print("⚠️ 批量增量返回空，逐支降级…")
                for sym in batch:
                    last_date = dict(need_incr)[sym]
                    df = fetch_one_with_backoff(sym, last_date + timedelta(days=1), today)
                    if df.empty:
                        print(f"[{sym}] 无增量数据")
                        continue
                    df = df.dropna(subset=["Close"]).reset_index()
                    history = price_data.get(sym, [])
                    new_entries = build_entries(df, last_date)
                    price_data[sym] = clean_and_sort(history + new_entries)
                    safe_sleep()
            else:
                # 解析批量
                frames = parse_download_frame(df_all, batch)
                for sym in batch:
                    last_date = dict(need_incr)[sym]
                    df_sym = frames.get(sym, pd.DataFrame())
                    if df_sym.empty:
                        print(f"[{sym}] 无增量数据")
                        continue
                    history = price_data.get(sym, [])
                    new_entries = build_entries(df_sym, last_date)
                    price_data[sym] = clean_and_sort(history + new_entries)

            # 批次落盘
            save_price_data(price_data)
            safe_sleep()

    # 2) 再处理全量（分批 period='max'）
    if need_full:
        for batch in chunked(need_full, CHUNK_SIZE):
            print(f"🧱 批量全量 {len(batch)} 支（period='max'）")
            df_all = download_with_backoff(
                tickers=batch,
                period="max",
                interval=INTERVAL,
                group_by="ticker",
                threads=YFINANCE_THREADS,
                auto_adjust=AUTO_ADJUST,
            )
            if df_all.empty:
                print("⚠️ 批量全量返回空，逐支降级…")
                for sym in batch:
                    df = fetch_one_with_backoff(sym)
                    if df.empty:
                        print(f"[{sym}] 全量无数据")
                        continue
                    df = df.dropna(subset=["Close"]).reset_index()
                    entries = build_entries(df, last_date=None)
                    price_data[sym] = clean_and_sort(entries)
                    safe_sleep()
            else:
                frames = parse_download_frame(df_all, batch)
                for sym in batch:
                    df_sym = frames.get(sym, pd.DataFrame())
                    if df_sym.empty:
                        print(f"[{sym}] 全量无数据")
                        continue
                    entries = build_entries(df_sym, last_date=None)
                    price_data[sym] = clean_and_sort(entries)

            # 批次落盘
            save_price_data(price_data)
            safe_sleep()

    # 最终保存
    save_price_data(price_data)
    print(f"🎉 更新完成，保存到 {PRICE_JSON_PATH}")
    return price_data


if __name__ == "__main__":
    fetch_and_update_prices()
