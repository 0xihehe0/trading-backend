'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:42
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-05-05 20:55:13
FilePath: \backend\app\routes\stock.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

stock_bp = Blueprint('stock', __name__)

# 加载数据（只需执行一次）
with open("data/sp500.json", "r") as f:
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

# MA推荐表（维持你原先的逻辑）
ma_recommendation_by_range = {
    "1d":  [5],
    "5d":  [5, 10],
    "1mo": [5, 10, 20],
    "6mo": [20, 50],
    "1y":  [50, 100],
    "2y":  [50, 100, 200],
}

@stock_bp.route('/api/stock', methods=['GET'])
def get_stock_data():
    symbol    = request.args.get("ticker", "AAPL").upper()
    range_key = request.args.get("range",  "6mo")
    ma_param  = request.args.get("ma",     "")
    ma_values = [int(x) for x in ma_param.split(',') if x.strip().isdigit()]

    # 1. 原始全量数据
    df = pd.DataFrame(stock_data[symbol])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    # ============================
    # 🔄 先算 MA，再切片
    for ma in ma_values:
        df[f"ma{ma}"] = df['close'].rolling(window=ma).mean()
    # ============================

    # —— 下面才开始按 range_key 截取 —— 

    date_range_offset = {
      "1d":  {"days": 1},
      "5d":  {"days": 5},
      "1mo": {"months": 1},
      "6mo": {"months": 6},
      "1y":  {"years": 1},
      "2y":  {"years": 2},
    }
    offset = date_range_offset.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df.index >= cutoff]
    # —— 切片结束 —— 

    # 其余不动：你可以不再 dropna()，让前端拿到 NaN 或直接省掉这个步骤
    df.reset_index(inplace=True)

    result = []
    for _, row in df.iterrows():
        item = {
            "date":  str(row['date'].date()),
            "close": round(row['close'], 2)
        }
        for ma in ma_values:
            key = f"ma{ma}"
            # 如果历史够，它现在就会有值
            if pd.notna(row.get(key)):
                item[key] = round(row[key], 2)
        result.append(item)

    return jsonify(result)

