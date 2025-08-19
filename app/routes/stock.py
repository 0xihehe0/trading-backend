'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:42
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-08-13 00:35:51
FilePath: /backend/app/routes/stock.py
Description: 增量按需更新 + 兼容原有返回结构
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import os
import time
import random

# 如需增量更新，需要 yfinance
try:
    import yfinance as yf
    from yfinance.exceptions import YFRateLimitError
except Exception:
    yf = None
    YFRateLimitError = Exception  # 占位，避免未安装时报错

stock_bp = Blueprint('stock', __name__)

DATA_PATH = "data/sp500_prices.json"

# 加载数据（只需执行一次）
with open(DATA_PATH, "r", encoding="utf-8") as f:
    stock_data = json.load(f)

# 支持的时间偏移映射
date_range_offset = {
    "1d":  {"days": 1},
    "5d":  {"days": 5},
    "1mo": {"months": 1},
    "6mo": {"months": 6},
    "1y":  {"years": 1},
    "2y":  {"years": 2},
}

# MA推荐表（保留）
ma_recommendation_by_range = {
    "1d":  [5],
    "5d":  [5, 10],
    "1mo": [5, 10, 20],
    "6mo": [20, 50],
    "1y":  [50, 100],
    "2y":  [50, 100, 200],
}

# ======== 实用函数 ========

def _safe_sleep(base=1.0, jitter=4.0):
    time.sleep(base + random.random() * jitter)  # 1~5秒随机

def _atomic_save_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

def _last_local_date(symbol: str):
    rows = stock_data.get(symbol, [])
    if not rows:
        return None
    try:
        return datetime.strptime(rows[-1]["date"], "%Y-%m-%d").date()
    except Exception:
        return None

def _expected_last_trading_date(today=None):
    """简单处理：周末回滚到最近工作日；（节假日可后续接入交易日历库）"""
    if today is None:
        today = datetime.now().date()
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd == 5:   # Saturday -> Friday
        return today - timedelta(days=1)
    if wd == 6:   # Sunday -> Friday
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
    """合并去重并按日期排序"""
    seen = set()
    out = []
    for e in (history + new_entries):
        dt = e["date"]
        if dt not in seen:
            seen.add(dt)
            out.append(e)
    out.sort(key=lambda x: x["date"])
    return out

def _fetch_increment(symbol: str, start_date, end_date, backfill_days=10):
    """
    仅获取增量：从 (start_date - backfill_days) 到 end_date
    backfill_days 做轻回刷，容错拆股/复权后的历史修正
    """
    if yf is None:
        return []

    start_q = (start_date - timedelta(days=backfill_days))
    wait = 2
    while True:
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
                # 仅合入真正增量（> 原 last_date）
                if d > start_date:
                    out.append({
                        "date": d.strftime("%Y-%m-%d"),
                        "open":  _to_float(r.get("Open")),
                        "high":  _to_float(r.get("High")),
                        "low":   _to_float(r.get("Low")),
                        "close": _to_float(r.get("Close")),
                        "volume": _to_int(r.get("Volume")),
                    })
            return out
        except YFRateLimitError:
            # 限流：指数退避
            time.sleep(wait)
            wait = min(wait * 2, 60)
        except Exception:
            # 其他异常：小睡后重试几次（这里简单处理为返回空）
            _safe_sleep()
            return []

def _ensure_fresh(symbol: str, autoupdate: bool = True) -> dict:
    """
    检查是否最新；若不是并且 autoupdate=True，则做增量更新并落盘。
    返回 {updated: bool, added_rows: int, message: str}
    """
    if symbol not in stock_data:
        return {"updated": False, "added_rows": 0, "message": f"{symbol} not found locally"}

    last_local = _last_local_date(symbol)
    if not last_local:
        # 本地完全没有该 symbol
        if autoupdate and yf is not None:
            # 全量抓取
            inc = _fetch_increment(symbol, start_date=datetime(1970,1,1).date(), end_date=_expected_last_trading_date())
            if inc:
                stock_data[symbol] = _merge_entries([], inc)
                _atomic_save_json(DATA_PATH, stock_data)
                return {"updated": True, "added_rows": len(inc), "message": "full fetched"}
            else:
                return {"updated": False, "added_rows": 0, "message": "no data fetched"}
        return {"updated": False, "added_rows": 0, "message": "no local data"}

    # 计算应有的最后交易日
    expected_last = _expected_last_trading_date()
    if last_local >= expected_last:
        return {"updated": False, "added_rows": 0, "message": "already up-to-date"}

    if not autoupdate or yf is None:
        return {"updated": False, "added_rows": 0, "message": "outdated but autoupdate disabled or yfinance missing"}

    # 增量补齐（last_local+1 → expected_last）
    inc = _fetch_increment(symbol, start_date=last_local, end_date=expected_last)
    if not inc:
        return {"updated": False, "added_rows": 0, "message": "no increment fetched"}

    merged = _merge_entries(stock_data.get(symbol, []), inc)
    stock_data[symbol] = merged
    _atomic_save_json(DATA_PATH, stock_data)
    return {"updated": True, "added_rows": len(inc), "message": "increment merged"}

# ======== 路由：获取价格（保留原有返回结构）+ 按需更新 ========

@stock_bp.route('/api/stock', methods=['GET'])
def get_stock_data():
    symbol    = request.args.get("ticker", "AAPL").upper()
    range_key = request.args.get("range",  "6mo")
    ma_param  = request.args.get("ma",     "")
    autoupdate = request.args.get("autoupdate", "1")  # "1"|"0"
    do_update = (autoupdate != "0")
    
    if symbol not in stock_data:
        return jsonify({"error": f"{symbol} not found"}), 404

    # 先做“是否最新”的判断，如需则增量更新本地缓存
    freshness = _ensure_fresh(symbol, autoupdate=do_update)
    # （可选）你也可以把 freshness 作为响应 header 或 debug 输出
    
    print(freshness)

    ma_values = [int(x) for x in ma_param.split(',') if x.strip().isdigit()]

    # 1) 原始全量数据（此时 stock_data 可能已被更新）
    df = pd.DataFrame(stock_data[symbol])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # 2) 先算 MA，再切片（确保短窗口也能显示长均线）
    for ma in ma_values:
        df[f"ma{ma}"] = df['close'].rolling(window=ma).mean()

    # 3) 再按 range 切片
    offset = date_range_offset.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df.index >= cutoff]

    df.reset_index(inplace=True)

    # 4) 输出仍然为 list（与原先完全一致）
    result = []
    for _, row in df.iterrows():
        item = {
            "date":  str(row['date'].date()),
            "close": round(row['close'], 2)
        }
        for ma in ma_values:
            key = f"ma{ma}"
            if pd.notna(row.get(key)):
                item[key] = round(row[key], 2)
        result.append(item)

    return jsonify(result)

# ======== 路由：批量更新（前端勾选后触发） ========

# @stock_bp.route('/api/stock/update', methods=['POST'])
# def update_stocks():
#     try:
#         payload = request.get_json(force=True) or {}
#         tickers = payload.get("tickers", [])
#         if not isinstance(tickers, list) or not tickers:
#             return jsonify({"error": "tickers must be a non-empty list"}), 400

#         results = []
#         for sym in tickers:
#             sym = str(sym).upper().strip()
#             if not sym:
#                 continue
#             info = _ensure_fresh(sym, autoupdate=True)
#             results.append({
#                 "symbol": sym,
#                 **info
#             })
#             _safe_sleep()  # 批量更新间隔，降低限流概率

#         return jsonify({
#             "updated": results,
#             "saved_to": DATA_PATH
#         })
#     except Exception as e:
#         return jsonify({"error": f"update error: {str(e)}"}), 500
