'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-19 20:16:40
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-19 20:23:25
FilePath: \backend\app\routes\backtest.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
import json
from app.services.signal_ma_cross import ma_cross_strategy
from app.services.backtest_service import backtest_ma_cross_strategy

strategy_bp = Blueprint('strategy', __name__)

# 加载股票数据
with open("data/sp500.json", "r") as f:
    all_data = json.load(f)

date_range_days = {
    "1d": 1, "5d": 5, "1mo": 30, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825, "max": None
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

    if range_key in date_range_days and date_range_days[range_key] is not None:
        cutoff = datetime.now() - timedelta(days=date_range_days[range_key])
        df = df[df.index >= cutoff]

    df.reset_index(inplace=True)

    if strategy_name == "ma_cross":
        short = params.get("short_ma", 50)
        long = params.get("long_ma", 200)
        signals = ma_cross_strategy(df, short, long)
        return jsonify(signals)

    return jsonify({"error": "unknown strategy"}), 400

@strategy_bp.route("/api/backtest", methods=["POST"])
def backtest_strategy():
    """
    执行策略回测的接口
    
    请求参数:
    - ticker: 股票代码 (例如 "AAPL")
    - range: 回测时间范围 (例如 "1y", "2y", "5y", "max")
    - strategy: 策略名称 (例如 "ma_cross")
    - params: 策略参数 (例如 {"short_ma": 50, "long_ma": 200})
    - initial_capital: 初始资金 (例如 10000)
    - commission: 交易手续费率 (例如 0.001)
    """
    payload = request.get_json()
    symbol = payload.get("ticker", "AAPL").upper()
    range_key = payload.get("range", "1y")
    strategy_name = payload.get("strategy", "ma_cross")
    params = payload.get("params", {})
    initial_capital = payload.get("initial_capital", 10000)
    commission = payload.get("commission", 0.001)
    
    # 检查股票代码是否存在
    if symbol not in all_data:
        return jsonify({"error": f"{symbol} not found"}), 404
        
    # 加载数据
    df = pd.DataFrame(all_data[symbol])
    df['date'] = pd.to_datetime(df['date'])
    
    # 应用时间范围过滤
    if range_key in date_range_days and date_range_days[range_key] is not None:
        cutoff = datetime.now() - timedelta(days=date_range_days[range_key])
        df = df[df['date'] >= cutoff]
    
    # 确保数据按日期排序
    df = df.sort_values('date')
    
    # 根据策略名称执行回测
    if strategy_name == "ma_cross":
        short_ma = params.get("short_ma", 50)
        long_ma = params.get("long_ma", 200)
        backtest_results = backtest_ma_cross_strategy(
            df, 
            initial_capital=initial_capital,
            short=short_ma,
            long=long_ma,
            commission=commission
        )
        return jsonify(backtest_results)
    
    return jsonify({"error": "unknown strategy"}), 400