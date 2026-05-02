# app/routes/newstock.py
"""
POST /api/newstock  { ticker, lastDay }

后端直接代理请求 Yahoo Finance（服务端请求无跨域问题），
拉取 lastDay+1 ~ 今天的增量数据，追加到本地 sp500_split/{ticker}.json，
返回 { data, meta } 结构，前端可直接刷新图表。
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import requests as http_requests
import json
import os
from app.config import get_data_path

newstock_bp = Blueprint("newstock", __name__)

# 伪装成浏览器，避免被 Yahoo 拒绝
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def _fetch_yahoo(ticker: str, period1: int, period2: int) -> list[dict]:
    """
    服务端请求 Yahoo Finance v8 API，返回标准格式记录列表。
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={period1}&period2={period2}&interval=1d"
    )

    resp = http_requests.get(url, headers=YAHOO_HEADERS, timeout=15)
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


@newstock_bp.route("/api/newstock", methods=["POST"])
def newstock():
    try:
        payload = request.get_json()
        ticker = payload.get("ticker", "").upper().strip()
        last_day = payload.get("lastDay")  # "2025-04-17" 或 None

        if not ticker:
            return jsonify({"error": "缺少 ticker 参数"}), 400

        filepath = get_data_path(ticker)

        # 读取现有数据
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                existing = json.load(f)
        else:
            existing = []

        # 计算拉取范围
        if last_day:
            start_dt = datetime.strptime(last_day, "%Y-%m-%d") + timedelta(days=1)
        else:
            start_dt = datetime(1990, 1, 1)

        end_dt = datetime.now()

        # 如果已经是最新，直接返回
        if last_day and start_dt.date() > end_dt.date():
            return jsonify({
                "data": existing,
                "meta": {
                    "symbol": ticker,
                    "status": "already_latest",
                    "message": "数据已是最新",
                    "last_date": last_day,
                    "total_records": len(existing),
                }
            })

        period1 = int(start_dt.timestamp())
        period2 = int(end_dt.timestamp())

        # 后端代理请求 Yahoo Finance
        new_records = _fetch_yahoo(ticker, period1, period2)

        # 去重
        existing_dates = {r["date"] for r in existing}
        new_records = [r for r in new_records if r["date"] not in existing_dates]

        if not new_records:
            return jsonify({
                "data": existing,
                "meta": {
                    "symbol": ticker,
                    "status": "no_new_data",
                    "message": "Yahoo 无新增交易日数据",
                    "last_date": existing[-1]["date"] if existing else None,
                    "total_records": len(existing),
                }
            })

        # 合并、排序、写入
        merged = existing + new_records
        merged.sort(key=lambda x: x["date"])

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False)

        last_date = merged[-1]["date"]

        return jsonify({
            "data": merged,
            "meta": {
                "symbol": ticker,
                "status": "updated",
                "message": f"新增 {len(new_records)} 条记录",
                "new_records": len(new_records),
                "total_records": len(merged),
                "last_date": last_date,
            }
        })

    except http_requests.exceptions.RequestException as e:
        return jsonify({"error": f"Yahoo Finance 请求失败: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": f"更新失败: {str(e)}"}), 500