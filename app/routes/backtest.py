from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
from app.services.signal_ma_cross import ma_cross_strategy
from app.services.backtest_service import backtest_ma_cross_strategy

backtest_bp = Blueprint('backtest', __name__)

# 加载本地股票数据
with open("data/sp500_prices.json", "r") as f:
    stock_data_dict = json.load(f)

date_range_offset = {
    "1d":  {"days": 1},
    "5d":  {"days": 5},
    "1mo": {"months": 1},
    "6mo": {"months": 6},
    "1y":  {"years": 1},
    "2y":  {"years": 2},
}


def load_stock_df(symbol, range_key):
    df = pd.DataFrame(stock_data_dict[symbol])
    df['date'] = pd.to_datetime(df['date'])

    if offset := date_range_offset.get(range_key):
        # 用 relativedelta，而不是 timedelta(days=offset_dict)
        cutoff = datetime.now() - relativedelta(**offset)
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
    try:
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

        if df.empty:
            return jsonify({"error": f"No data available for {symbol} in range {date_range_key}"}), 400

        result = {
            "symbol": symbol,
            "strategy": strategy_name,
            "range": date_range_key,
            "params": {"short_ma": short_ma, "long_ma": long_ma},
            "mode": mode
        }

        if strategy_name != "ma_cross":
            return jsonify({"error": f"Unsupported strategy: {strategy_name}"}), 400

        # 🔁 信号部分
        if mode in ("signal", "both"):
            try:
                signals = ma_cross_strategy(df.copy(), short=short_ma, long=long_ma)
                result["signals"] = signals
            except Exception as e:
                result["signals"] = []
                result["signals_error"] = f"Signal generation failed: {str(e)}"

        # 📊 回测部分
        if mode in ("backtest", "both"):
            try:
                backtest_result = backtest_ma_cross_strategy(
                    df.copy(),
                    initial_capital=initial_capital,
                    short=short_ma,
                    long=long_ma,
                    commission=commission
                )
                result["backtest"] = backtest_result
            except Exception as e:
                result["backtest"] = {}
                result["backtest_error"] = f"Backtest failed: {str(e)}"

        return jsonify(result)

    except Exception as e:
        print(f"🔥 全局异常: {e}")
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500
