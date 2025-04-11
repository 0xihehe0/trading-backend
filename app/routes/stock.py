'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:42
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-09 21:52:33
FilePath: \backend\app\routes\stock.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import request, jsonify
import pandas as pd
from datetime import datetime, timedelta
import json

from flask import Blueprint

stock_bp = Blueprint('stock', __name__)

# 加载数据（只需执行一次）
with open("data/sp500.json", "r") as f:
    stock_data = json.load(f)

# 日期区间映射表
date_range_days = {
    "1d": 1,
    "5d": 5,
    "1mo": 30,
    "6mo": 180,
    "1y": 365,
    "2y": 730
}

# MA推荐表
ma_recommendation_by_range = {
    "1d": [5],
    "5d": [5, 10],
    "1mo": [5, 10, 20],
    "6mo": [20, 50],
    "1y": [50, 100],
    "2y": [50, 100, 200]
}

@stock_bp.route('/api/stock', methods=['GET'])
def get_stock_data():
    symbol = request.args.get("ticker", "AAPL").upper()
    range_key = request.args.get("range", "6mo")
    ma_param = request.args.get("ma", "")
    ma_values = [int(x.strip()) for x in ma_param.split(',') if x.strip().isdigit()]

    if symbol not in stock_data:
        return jsonify({"error": f"{symbol} not found"}), 404

    # 转 DataFrame 并处理时间
    df = pd.DataFrame(stock_data[symbol])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # 过滤区间数据
    if range_key in date_range_days:
        cutoff = datetime.now() - timedelta(days=date_range_days[range_key])
        df = df[df.index >= cutoff]

    if df.empty:
        return jsonify({"error": "No data available for this range"}), 400

    # 判断是否有足够数据计算 MA
    max_ma = max(ma_values) if ma_values else 0
    if max_ma > 0 and len(df) < max_ma:
        return jsonify({
            "error": f"Data range too short for MA{max_ma}.",
            "max_available_ma": len(df),
            "suggested_mas": [ma for ma in ma_recommendation_by_range.get(range_key, []) if ma <= len(df)]
        }), 400

    # 计算 MA
    for ma in ma_values:
        df[f"ma{ma}"] = df['close'].rolling(window=ma).mean()

    df.dropna(inplace=True)
    df.reset_index(inplace=True)

    result = []
    for _, row in df.iterrows():
        item = {
            "date": str(row['date'].date()),
            "close": round(row['close'], 2)
        }
        for ma in ma_values:
            ma_key = f"ma{ma}"
            if pd.notna(row.get(ma_key)):
                item[ma_key] = round(row[ma_key], 2)
        result.append(item)

    return jsonify(result)
