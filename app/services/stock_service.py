# app/services/stock_service.py
"""
职责：读取/缓存本地数据、按需增量更新（yfinance）、裁剪区间、计算均线。
对外暴露：get_stock_series(symbol, range_key, ma_values, autoupdate)
返回结构与旧 /api/stock 完全一致（list[dict]）
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import time, json, os, random
from threading import RLock

# ========== 可选依赖：yfinance ==========
try:
    import yfinance as yf
    from yfinance.exceptions import YFRateLimitError
except Exception:
    yf = None
    class YFRateLimitError(Exception):  # 占位
        pass

# ========== 基础配置 ==========
DATA_PATH = "data/sp500_prices.json"  # 仍沿用老存储结构，方便兼容
_INDENT_JSON = 2                      # 如需进一步加速，改成 None

# range → 时间偏移
DATE_RANGE_OFFSET = {
    "1d":  {"days": 1},
    "5d":  {"days": 5},
    "1mo": {"months": 1},
    "6mo": {"months": 6},
    "1y":  {"years": 1},
    "2y":  {"years": 2},
}

# ========== 线程安全缓存 ==========
_stock_cache = None     # dict: {symbol: [{date, open, high, low, close, volume}, ...]}
_cache_lock  = RLock()

# 针对 autoupdate 的防抖（避免短时间重复外网抓取）
_LAST_UPDATE_TS = {}
_UPDATE_LOCK    = RLock()
_UPDATE_DEBOUNCE_SEC = 300  # 5分钟内不重复抓同一只

# ========== 工具函数 ==========
def _safe_sleep(base=1.0, jitter=4.0):
    time.sleep(base + random.random() * jitter)  # 1~5 秒

def _atomic_save_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=_INDENT_JSON, ensure_ascii=False)
    os.replace(tmp, path)

def _load_data() -> dict:
    """惰性加载，首次读取后缓存到内存。"""
    global _stock_cache
    with _cache_lock:
        if _stock_cache is None:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                _stock_cache = json.load(f)
        return _stock_cache

def _save_data():
    with _cache_lock:
        if _stock_cache is not None:
            _atomic_save_json(DATA_PATH, _stock_cache)

def _get_rows(symbol: str):
    data = _load_data()
    return data.get(symbol, [])

def _set_rows(symbol: str, rows: list):
    data = _load_data()
    data[symbol] = rows
    _save_data()

def _last_local_date(symbol: str):
    rows = _get_rows(symbol)
    if not rows:
        return None
    try:
        return datetime.strptime(rows[-1]["date"], "%Y-%m-%d").date()
    except Exception:
        return None

def _expected_last_trading_date(today=None):
    """周末回滚。节假日后续接交易日历。"""
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd == 5:  # Sat
        return today - timedelta(days=1)
    if wd == 6:  # Sun
        return today - timedelta(days=2)
    return today

def _to_float(x):
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

def _merge_entries(history: list, new_entries: list) -> list:
    seen = set()
    out = []
    for e in (history + new_entries):
        dt = e["date"]
        if dt not in seen:
            seen.add(dt)
            out.append(e)
    out.sort(key=lambda x: x["date"])
    return out

def _can_update(symbol: str) -> bool:
    with _UPDATE_LOCK:
        last = _LAST_UPDATE_TS.get(symbol, 0)
        now = time.time()
        if now - last < _UPDATE_DEBOUNCE_SEC:
            return False
        _LAST_UPDATE_TS[symbol] = now
        return True

# ========== yfinance 增量抓取（修复无限重试） ==========
def _fetch_increment(symbol: str, start_date, end_date, backfill_days=10,
                     max_attempts=4, budget_seconds=12):
    """
    仅获取增量：从 (start_date - backfill_days) 到 end_date
    增加最大重试 & 总时长预算，避免卡死。
    """
    if yf is None:
        return []

    start_q = (start_date - timedelta(days=backfill_days))
    wait = 2
    attempts = 0
    deadline = time.time() + budget_seconds

    while attempts < max_attempts and time.time() < deadline:
        attempts += 1
        try:
            df = yf.Ticker(symbol).history(
                start=start_q.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                interval="1d",
                auto_adjust=True
            )
            if df is None or df.empty:
                return []
            df = df.dropna(subset=["Close"]).reset_index()
            out = []
            for _, r in df.iterrows():
                d = r["Date"].date()
                if d > start_date:
                    out.append({
                        "date":   d.strftime("%Y-%m-%d"),
                        "open":   _to_float(r.get("Open")),
                        "high":   _to_float(r.get("High")),
                        "low":    _to_float(r.get("Low")),
                        "close":  _to_float(r.get("Close")),
                        "volume": _to_int(r.get("Volume")),
                    })
            return out
        except YFRateLimitError:
            time.sleep(wait)
            wait = min(wait * 2, 8)
            continue
        except Exception:
            _safe_sleep()
            return []
    return []  # 超预算或重试完

def ensure_fresh(symbol: str, autoupdate: bool = True) -> dict:
    """
    检查是否最新；必要时增量更新并落盘。
    返回 {updated: bool, added_rows: int, message: str}
    """
    data = _load_data()
    if symbol not in data:
        return {"updated": False, "added_rows": 0, "message": f"{symbol} not found locally"}

    last_local = _last_local_date(symbol)
    if not last_local:
        if autoupdate and yf is not None and _can_update(symbol):
            inc = _fetch_increment(symbol, start_date=datetime(1970,1,1).date(),
                                   end_date=_expected_last_trading_date())
            if inc:
                _set_rows(symbol, _merge_entries([], inc))
                return {"updated": True, "added_rows": len(inc), "message": "full fetched"}
            return {"updated": False, "added_rows": 0, "message": "no data fetched"}
        return {"updated": False, "added_rows": 0, "message": "no local data"}

    expected_last = _expected_last_trading_date()
    if last_local >= expected_last:
        return {"updated": False, "added_rows": 0, "message": "already up-to-date"}

    if not autoupdate or yf is None or not _can_update(symbol):
        return {"updated": False, "added_rows": 0, "message": "outdated; autoupdate off or yfinance missing or debounced"}

    inc = _fetch_increment(symbol, start_date=last_local, end_date=expected_last)
    if not inc:
        return {"updated": False, "added_rows": 0, "message": "no increment fetched"}

    merged = _merge_entries(_get_rows(symbol), inc)
    _set_rows(symbol, merged)
    return {"updated": True, "added_rows": len(inc), "message": "increment merged"}

def get_stock_series(symbol: str, range_key: str, ma_values: list[int], autoupdate: bool):
    """
    返回路由所需的 list[dict] 结构，兼容旧接口。
    """
    symbol = str(symbol).upper().strip()
    data = _load_data()
    if symbol not in data:
        return None, {"error": f"{symbol} not found"}

    # 1) 可选：先刷新
    t0 = time.time()
    freshness = ensure_fresh(symbol, autoupdate=autoupdate)
    print("[stock_service] ensure_fresh:", freshness, "cost=", round(time.time()-t0, 3), "s", flush=True)

    # 2) 算 MA，再做区间裁剪
    df = pd.DataFrame(_get_rows(symbol))
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    for ma in ma_values:
        df[f"ma{ma}"] = df["close"].rolling(window=ma).mean()

    offset = DATE_RANGE_OFFSET.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df.index >= cutoff]

    df.reset_index(inplace=True)

    # 3) 输出 list
    out = []
    for _, row in df.iterrows():
        item = {"date": str(row["date"].date()), "close": round(row["close"], 2)}
        for ma in ma_values:
            k = f"ma{ma}"
            v = row.get(k)
            if pd.notna(v):
                item[k] = round(v, 2)
        out.append(item)

    return out, freshness
