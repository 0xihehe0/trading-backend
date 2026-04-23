'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-19 20:16:40
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2026-04-23 10:14:04
FilePath: \backend\app\routes\backtest.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json, os
from app.config import DATA_DIR, DATE_RANGE_OFFSET
from app.services.signal_ma_cross import ma_cross_strategy
from app.services.backtest_service import backtest_ma_cross_strategy

backtest_bp = Blueprint('backtest', __name__)

def _load_symbol(symbol: str) -> list:
    filepath = os.path.join(DATA_DIR, f"{symbol}.json")
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_stock_df(symbol, range_key):
    rows = _load_symbol(symbol)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])

    if offset := DATE_RANGE_OFFSET.get(range_key):
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df['date'] >= cutoff]

    return df.sort_values('date').reset_index(drop=True)


@backtest_bp.route("/api/strategy_backtest_combined", methods=["POST"])
def strategy_backtest_combined():
    """
    多功能策略接口：支持信号生成 + 回测结果
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

        # ===== 改动：不再检查 stock_data_dict，直接加载试试 =====
        df = load_stock_df(symbol, date_range_key)

        if df.empty:
            return jsonify({"error": f"No data available for {symbol} in range {date_range_key}"}), 404

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