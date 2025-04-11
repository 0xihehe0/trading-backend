'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:49
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-08 21:48:21
FilePath: \backend\app\routes\strategy.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
import json
from app.services.signal_ma_cross import ma_cross_strategy

strategy_bp = Blueprint('strategy', __name__)
with open("data/sp500.json", "r") as f:
    all_data = json.load(f)

date_range_days = {
    "1d": 1, "5d": 5, "1mo": 30, "6mo": 180, "1y": 365, "2y": 730
}

@strategy_bp.route("/api/strategy", methods=["POST"])
def compute_strategy():
    payload = request.get_json()
    symbol = payload.get("ticker", "AAPL").upper()
    range_key = payload.get("range", "6mo")
    strategy_name = payload.get("strategy")
    params = payload.get("params", {})

    if symbol not in all_data:
        return jsonify({"error": f"{symbol} not found"}), 404

    df = pd.DataFrame(all_data[symbol])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    if range_key in date_range_days:
        cutoff = datetime.now() - timedelta(days=date_range_days[range_key])
        df = df[df.index >= cutoff]

    df.reset_index(inplace=True)

    if strategy_name == "ma_cross":
        short = params.get("short_ma", 50)
        long = params.get("long_ma", 200)
        signals = ma_cross_strategy(df, short, long)
        return jsonify(signals)

    return jsonify({"error": "unknown strategy"}), 400
