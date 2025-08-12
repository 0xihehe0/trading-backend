import os
import json
import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
import time
import random
from datetime import datetime, timedelta

# ========== 配置区 ==========
CSV_PATH = "data/sp500_symbols.csv"
PRICE_JSON_PATH = "data/sp500_prices.json"
OUTPUT_DIR = os.path.dirname(PRICE_JSON_PATH)
# 单次批量请求线程数（False 禁用并发）
YFINANCE_THREADS = False
# 指数退避初始等待和最大等待
BACKOFF_INITIAL = 1
BACKOFF_MAX = 60
# 请求间隔（随机抖动）
SLEEP_BASE = 5
SLEEP_JITTER = 5

# ========== 工具函数 ==========
def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def safe_sleep():
    time.sleep(SLEEP_BASE + random.random() * SLEEP_JITTER)

def load_symbols():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Symbol list CSV not found: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    return df['Symbol'].dropna().str.replace('.', '-', regex=False).str.strip().tolist()

def load_price_data():
    if os.path.exists(PRICE_JSON_PATH):
        with open(PRICE_JSON_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_price_data(data):
    with open(PRICE_JSON_PATH, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_with_backoff(symbol, start_date=None, end_date=None):
    """单个符号增量获取，带指数退避"""
    wait = BACKOFF_INITIAL
    while True:
        try:
            if start_date:
                df = yf.Ticker(symbol).history(
                    start=start_date.strftime("%Y-%m-%d"),
                    end=end_date.strftime("%Y-%m-%d"),
                    interval='1d'
                )
            else:
                df = yf.Ticker(symbol).history(
                    period='max',
                    interval='1d'
                )
            return df
        except YFRateLimitError:
            print(f"[{symbol}] 被限流, 等待 {wait}s 再试")
            time.sleep(wait)
            wait = min(wait * 2, BACKOFF_MAX)
        except Exception as e:
            print(f"[{symbol}] 获取失败: {e}")
            return pd.DataFrame()

# ========== 主逻辑 ==========
def fetch_and_update_prices():
    ensure_directory_exists(OUTPUT_DIR)
    symbols = load_symbols()
    price_data = load_price_data()
    today = datetime.now().date()

    # 确定需要更新的符号及最早增量日期
    to_update = []
    earliest = today
    for sym in symbols:
        history = price_data.get(sym, [])
        if history:
            try:
                last_date = datetime.strptime(history[-1]['date'], "%Y-%m-%d").date()
            except:
                last_date = None
        else:
            last_date = None
        if not last_date or last_date < today:
            to_update.append((sym, last_date))
            if last_date and last_date + timedelta(days=1) < earliest:
                earliest = last_date + timedelta(days=1)

    if not to_update:
        print("✅ 所有数据已是最新")
        return price_data

    # 尝试一次性批量下载
    syms = [sym for sym, ld in to_update]
    try:
        print(f"🔀 批量下载 {len(syms)} 只股票数据")
        if all(ld for _, ld in to_update):
            df_all = yf.download(
                tickers=syms,
                start=earliest.strftime("%Y-%m-%d"),
                end=today.strftime("%Y-%m-%d"),
                interval='1d',
                group_by='ticker',
                threads=YFINANCE_THREADS
            )
        else:
            df_all = yf.download(
                tickers=syms,
                period='max',
                interval='1d',
                group_by='ticker',
                threads=YFINANCE_THREADS
            )
        if df_all.empty:
            raise YFRateLimitError("空结果，可能被限流")
        # 解析批量数据
        for sym, last_date in to_update:
            history = price_data.get(sym, [])
            df_sym = df_all[sym] if sym in df_all.columns.levels[0] else (df_all if 'Close' in df_all.columns else pd.DataFrame())
            if df_sym.empty:
                print(f"[{sym}] 无返回数据")
                continue
            df_sym = df_sym.dropna(subset=['Close']).reset_index()
            new_entries = []
            for _, row in df_sym.iterrows():
                d = row['Date'].date()
                if not last_date or d > last_date:
                    new_entries.append({
                        'date': d.strftime("%Y-%m-%d"),
                        'open': round(row['Open'], 2),
                        'high': round(row['High'], 2),
                        'low': round(row['Low'], 2),
                        'close': round(row['Close'], 2),
                        'volume': int(row['Volume'])
                    })
            price_data[sym] = (history + new_entries)
        print("✅ 批量更新完成")
    except YFRateLimitError as e:
        print(f"⚠️ 批量限流，切换到单个符号增量模式: {e}")
        for sym, last_date in to_update:
            print(f"🌐 更新 {sym}")
            df = fetch_with_backoff(sym, start_date=(last_date + timedelta(days=1)) if last_date else None, end_date=today)
            if df.empty:
                print(f"[{sym}] 无数据")
                continue
            df = df.dropna(subset=['Close']).reset_index()
            history = price_data.get(sym, [])
            new_entries = []
            for _, row in df.iterrows():
                d = row['Date'].date()
                if not last_date or d > last_date:
                    new_entries.append({
                        'date': d.strftime("%Y-%m-%d"),
                        'open': round(row['Open'], 2),
                        'high': round(row['High'], 2),
                        'low': round(row['Low'], 2),
                        'close': round(row['Close'], 2),
                        'volume': int(row['Volume'])
                    })
            price_data[sym] = (history + new_entries)
            safe_sleep()

    # 去重 & 排序
    for sym, entries in price_data.items():
        seen = set()
        filtered = []
        for e in entries:
            if e['date'] not in seen:
                seen.add(e['date'])
                filtered.append(e)
        filtered.sort(key=lambda x: x['date'])
        price_data[sym] = filtered

    # 保存
    save_price_data(price_data)
    print("🎉 更新并保存完成:", PRICE_JSON_PATH)
    return price_data

if __name__ == "__main__":
    fetch_and_update_prices()
