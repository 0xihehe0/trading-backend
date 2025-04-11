'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-11 22:16:48
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-11 22:16:54
FilePath: \backend\app\routes\symbols.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, jsonify
import pandas as pd

symbols_bp = Blueprint('symbols', __name__)

@symbols_bp.route('/api/symbols', methods=['GET'])
def get_symbols():
    try:
        df = pd.read_csv('data/sp500_symbols.csv')
        df = df.dropna(subset=['symbol', 'company'])

        results = [
            {"label": f"{row['company']} ({row['symbol']})", "value": row['symbol']}
            for _, row in df.iterrows()
        ]

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
