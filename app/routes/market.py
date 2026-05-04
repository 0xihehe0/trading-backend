# app/routes/market.py
"""
GET /api/market_overview?range=6mo

一次性返回市场总览数据：
  - S&P 500 (^GSPC) 行情 + 指标
  - VIX (^VIX) 行情 + 当前状态
  - CNN Fear & Greed 历史 + 当前状态

前端用这个接口渲染 Market Overview 页面。
"""

from flask import Blueprint, request, jsonify
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json, os

from app.config import get_data_path, DATE_RANGE_OFFSET

market_bp = Blueprint("market", __name__)


def _load_and_trim(symbol: str, range_key: str) -> list:
    """加载 JSON 并按时间范围裁剪，返回 list[dict]。"""
    filepath = get_data_path(symbol)
    if not os.path.exists(filepath):
        return []

    with open(filepath, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not rows:
        return []

    offset = DATE_RANGE_OFFSET.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        rows = [r for r in rows if r.get("date", "") >= cutoff_str]

    return rows


def _calc_change(rows: list) -> dict:
    """计算涨跌幅和区间统计。"""
    if len(rows) < 2:
        return {}

    first_close = float(rows[0].get("close", 0))
    last_close = float(rows[-1].get("close", 0))

    change = last_close - first_close
    change_pct = (change / first_close * 100) if first_close > 0 else 0

    closes = [float(r["close"]) for r in rows if r.get("close") is not None]

    return {
        "current": round(last_close, 2),
        "open": round(first_close, 2),
        "change": round(change, 2),
        "change_pct": round(change_pct, 2),
        "high": round(max(closes), 2),
        "low": round(min(closes), 2),
        "last_date": rows[-1].get("date"),
    }


def _vix_status(value: float) -> str:
    """VIX 情绪区间判断。"""
    if value < 12:
        return "Extremely Low"
    elif value < 20:
        return "Low"
    elif value < 25:
        return "Normal"
    elif value < 30:
        return "Elevated"
    elif value < 40:
        return "High"
    else:
        return "Extreme Fear"


def _fng_label(value: float) -> str:
    """Fear & Greed 标签。"""
    if value <= 25:
        return "Extreme Fear"
    elif value <= 45:
        return "Fear"
    elif value <= 55:
        return "Neutral"
    elif value <= 75:
        return "Greed"
    else:
        return "Extreme Greed"


@market_bp.route("/api/market_overview", methods=["GET"])
def market_overview():
    range_key = request.args.get("range", "6mo")

    result = {"range": range_key}

    # ===== S&P 500 =====
    sp500_rows = _load_and_trim("^GSPC", range_key)
    if sp500_rows:
        sp500_stats = _calc_change(sp500_rows)
        # 简化数据点：只返回 date 和 close，前端画线够用
        sp500_chart = [{"date": r["date"], "close": round(float(r["close"]), 2)}
                       for r in sp500_rows if r.get("close") is not None]
        result["sp500"] = {
            "stats": sp500_stats,
            "chart": sp500_chart,
        }
    else:
        result["sp500"] = {"error": "^GSPC 数据不可用"}

    # ===== VIX =====
    vix_rows = _load_and_trim("^VIX", range_key)
    if vix_rows:
        vix_stats = _calc_change(vix_rows)
        vix_current = float(vix_rows[-1].get("close", 0))
        vix_stats["status"] = _vix_status(vix_current)

        vix_chart = [{"date": r["date"], "close": round(float(r["close"]), 2)}
                     for r in vix_rows if r.get("close") is not None]
        result["vix"] = {
            "stats": vix_stats,
            "chart": vix_chart,
        }
    else:
        result["vix"] = {"error": "^VIX 数据不可用"}

    # ===== Fear & Greed =====
    fng_rows = _load_and_trim("FEAR_GREED", range_key)
    if fng_rows:
        # Fear & Greed 格式: {date, value, label}
        last = fng_rows[-1]
        fng_current = float(last.get("value", 50))
        fng_chart = [{"date": r["date"], "value": round(float(r["value"]), 1)}
                     for r in fng_rows if r.get("value") is not None]

        result["fear_greed"] = {
            "stats": {
                "current": round(fng_current, 1),
                "label": _fng_label(fng_current),
                "last_date": last.get("date"),
                "high": round(max(float(r["value"]) for r in fng_rows), 1),
                "low": round(min(float(r["value"]) for r in fng_rows), 1),
            },
            "chart": fng_chart,
        }
    else:
        result["fear_greed"] = {"error": "Fear & Greed 数据不可用"}

    return jsonify(result)