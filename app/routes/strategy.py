'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:49
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2026-04-23 10:14:17
FilePath: \backend\app\routes\strategy.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json, os
from app.config import DATA_DIR, DATE_RANGE_OFFSET
from app.services.signal_ma_cross import ma_cross_strategy

strategy_bp = Blueprint('strategy', __name__)

def _load_symbol(symbol: str) -> list:
    filepath = os.path.join(DATA_DIR, f"{symbol}.json")
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

@strategy_bp.route("/api/strategy", methods=["POST"])
def compute_strategy():
    payload = request.get_json()
    symbol = payload.get("ticker", "AAPL").upper()
    range_key = payload.get("range", "6mo")
    strategy_name = payload.get("strategy")
    params = payload.get("params", {})

    rows = _load_symbol(symbol)
    if not rows:
        return jsonify({"error": f"{symbol} not found"}), 404

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    offset = DATE_RANGE_OFFSET.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df.index >= cutoff]

    df.reset_index(inplace=True)

    if strategy_name == "ma_cross":
        short = params.get("short_ma", 50)
        long = params.get("long_ma", 200)
        signals = ma_cross_strategy(df, short, long)
        return jsonify(signals)

    return jsonify({"error": "unknown strategy"}), 400