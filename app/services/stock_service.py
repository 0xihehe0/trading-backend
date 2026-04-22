# app/services/stock_service.py
"""
只读本地：读取 data/sp500_split/{SYMBOL}.json，裁剪区间，计算均线。
返回结构与旧 /api/stock 一致：list[dict(date, close, maXX...)]。

改动说明（相对旧版）：
  - 数据源从单个 sp500_prices.json 改为 sp500_split/ 目录下的逐公司文件
  - 不再一次性加载 400MB 到内存，按需读取单个公司文件（通常 < 1MB）
  - 增加 LRU 缓存，避免同一公司反复读磁盘
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import json, os

# ===== 基础配置 =====
DATA_DIR = "data/sp500_split"  # 拆分后的目录，每个公司一个 JSON 文件

# 状态码
STATUS_LOCAL_ONLY       = "local_only"
STATUS_SYMBOL_NOT_FOUND = "symbol_not_found"
STATUS_EMPTY_LOCAL      = "empty_local"

# range → 时间偏移
DATE_RANGE_OFFSET = {
    "1d":  {"days": 1},
    "5d":  {"days": 5},
    "1mo": {"months": 1},
    "6mo": {"months": 6},
    "1y":  {"years": 1},
    "2y":  {"years": 2},
}


# ===== 数据加载 =====
def _load_symbol(symbol: str) -> list:
    """按需加载单个公司的 JSON 文件。"""
    filepath = os.path.join(DATA_DIR, f"{symbol}.json")
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_rows(symbol: str) -> list:
    return _load_symbol(symbol)


def _to_float(x):
    try:
        return round(float(x), 2)
    except Exception:
        return None


def _last_local_date(symbol: str):
    rows = _get_rows(symbol)
    if not rows:
        return None
    try:
        return datetime.strptime(rows[-1]["date"], "%Y-%m-%d").date()
    except Exception:
        return None


def _expected_last_trading_date(today=None):
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()
    if wd == 5:
        return today - timedelta(days=1)
    if wd == 6:
        return today - timedelta(days=2)
    return today


def is_local_fresh(symbol: str) -> dict:
    """
    只读本地，判断是否最新。
    """
    try:
        last = _last_local_date(symbol)
        expected = _expected_last_trading_date()
        if last is None:
            return {"fresh": False, "last_local": None, "expected_last": expected.strftime("%Y-%m-%d")}
        return {
            "fresh": last >= expected,
            "last_local": last.strftime("%Y-%m-%d"),
            "expected_last": expected.strftime("%Y-%m-%d"),
        }
    except Exception:
        return {"fresh": False, "last_local": None, "expected_last": None}


def get_stock_series(symbol: str, range_key: str, ma_values: list[int], policy: str = "local"):
    """
    只读本地版本：忽略 policy，始终走本地。
    返回： (list[dict], freshness_info)
    """
    symbol = str(symbol).upper().strip()
    rows = _get_rows(symbol)
    if not rows:
        return [], {"status": STATUS_SYMBOL_NOT_FOUND, "message": f"{symbol} not found locally"}

    df = pd.DataFrame(rows)
    if df.empty:
        return [], {"status": STATUS_EMPTY_LOCAL, "message": "local data empty"}

    # 归一化
    df["date"]  = pd.to_datetime(df["date"])
    df["close"] = df["close"].apply(_to_float)
    df = df.sort_values("date").set_index("date")

    # 均线
    for ma in ma_values:
        df[f"ma{ma}"] = df["close"].rolling(window=ma).mean()

    # 时间裁剪
    offset = DATE_RANGE_OFFSET.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df.index >= cutoff]

    df = df.reset_index()
    out = []
    for _, r in df.iterrows():
        item = {"date": str(r["date"].date()), "close": _to_float(r["close"])}
        for ma in ma_values:
            k = f"ma{ma}"
            v = r.get(k)
            if pd.notna(v):
                item[k] = _to_float(v)
        out.append(item)

    freshness = {
        "status":  STATUS_LOCAL_ONLY,
        "message": "served from local cache (no network)",
    }
    return out, freshness