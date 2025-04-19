from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
import json
from app.services.signal_ma_cross import ma_cross_strategy
from app.services.backtest_service import backtest_ma_cross_strategy

backtest_bp = Blueprint('strategy_combined', __name__)

# 加载本地股票数据
with open("data/sp500.json", "r") as f:
    stock_data_dict = json.load(f)

date_range_days = {
    "1d": 1, "5d": 5, "1mo": 30, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825, "max": None
}

def load_stock_df(symbol, range_key):
    df = pd.DataFrame(stock_data_dict[symbol])
    df['date'] = pd.to_datetime(df['date'])

    if date_range_key := date_range_days.get(range_key):
        cutoff = datetime.now() - timedelta(days=date_range_key)
        df = df[df['date'] >= cutoff]

    return df.sort_values('date').reset_index(drop=True)


@backtest_bp.route("/api/strategy_backtest_combined", methods=["POST"])
def strategy_backtest_combined():
    """
    多功能策略接口：支持信号生成 + 回测结果
    参数:
    - ticker: 股票代码
    - range: 时间范围，如 '6mo'
    - strategy: 策略名，例如 'ma_cross'
    - params: 策略参数
    - mode: 'signal' / 'backtest' / 'both'
    - initial_capital: 初始资金（仅在回测时需要）
    - commission: 手续费率（仅在回测时需要）
    """
    payload = request.get_json()
    symbol = payload.get("ticker", "AAPL").upper()
    date_range_key = payload.get("range", "6mo")
    strategy_name = payload.get("strategy", "ma_cross")
    mode = payload.get("mode", "both")

    params = payload.get("params", {})
    short_ma = params.get("short_ma", 50)
    long_ma = params.get("long_ma", 200)

    initial_capital = payload.get("initial_capital", 10000)
    commission = payload.get("commission", 0.001)

    if symbol not in stock_data_dict:
        return jsonify({"error": f"{symbol} not found"}), 404

    df = load_stock_df(symbol, date_range_key)

    result = {
        "symbol": symbol,
        "strategy": strategy_name,
        "range": date_range_key,
        "params": {"short_ma": short_ma, "long_ma": long_ma},
        "mode": mode
    }

    if strategy_name != "ma_cross":
        return jsonify({"error": f"Unsupported strategy: {strategy_name}"}), 400

    if mode in ("signal", "both"):
        signals = ma_cross_strategy(df.copy(), short=short_ma, long=long_ma)
        result["signals"] = signals

    if mode in ("backtest", "both"):
        backtest_result = backtest_ma_cross_strategy(
            df.copy(),
            initial_capital=initial_capital,
            short=short_ma,
            long=long_ma,
            commission=commission
        )
        result["backtest"] = backtest_result

    return jsonify(result)
