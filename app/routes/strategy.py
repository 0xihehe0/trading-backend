# app/routes/strategy.py
"""
统一策略路由，合并原 strategy.py + backtest.py。

三个接口:
    GET  /api/strategies                   → 返回所有可用策略及参数定义
    POST /api/strategy                     → 运行策略，返回信号
    POST /api/strategy_backtest_combined   → 信号 + 回测（保持旧接口兼容）

不再硬编码任何策略名称，通过注册中心动态查找。
新增策略只需在 app/strategies/ 下加文件，无需改本文件。
"""

from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json, os

from app.config import get_data_path, DATE_RANGE_OFFSET
from app.strategies import get_strategy, list_strategies
from app.services.backtest_engine import run_backtest

strategy_bp = Blueprint("strategy", __name__)


# ===== 工具函数 =====

def _load_stock_df(symbol: str, range_key: str) -> pd.DataFrame:
    """加载股票数据并按时间范围裁剪。"""
    filepath = get_data_path(symbol)
    if not os.path.exists(filepath):
        return pd.DataFrame()

    with open(filepath, "r", encoding="utf-8") as f:
        rows = json.load(f)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])

    offset = DATE_RANGE_OFFSET.get(range_key)
    if offset is not None:
        cutoff = datetime.now() - relativedelta(**offset)
        df = df[df["date"] >= cutoff]

    return df.sort_values("date").reset_index(drop=True)


def _fill_default_params(strategy_config: dict, user_params: dict) -> dict:
    """用策略 CONFIG 中的默认值填充用户未传的参数。"""
    filled = {}
    for p in strategy_config.get("params", []):
        key = p["key"]
        default = p["default"]
        value = user_params.get(key, default)
        # 类型转换
        if p.get("type") == "int":
            value = int(value)
        elif p.get("type") == "float":
            value = float(value)
        filled[key] = value
    return filled


# ===== 接口 =====

@strategy_bp.route("/api/strategies", methods=["GET"])
def get_strategies():
    """返回所有可用策略及其参数定义，前端用来动态渲染。"""
    return jsonify(list_strategies())


@strategy_bp.route("/api/strategy", methods=["POST"])
def compute_strategy():
    """
    运行策略，返回信号列表。

    请求体:
    {
        "ticker": "AAPL",
        "range": "2y",
        "strategy": "ma_cross",
        "params": {"short_ma": 50, "long_ma": 200}
    }
    """
    payload = request.get_json()
    symbol = payload.get("ticker", "AAPL").upper()
    range_key = payload.get("range", "6mo")
    strategy_name = payload.get("strategy", "ma_cross")
    user_params = payload.get("params", {})

    # 查找策略
    strategy = get_strategy(strategy_name)
    if strategy is None:
        available = [s["name"] for s in list_strategies()]
        return jsonify({
            "error": f"未知策略: {strategy_name}",
            "available": available,
        }), 400

    # 加载数据
    df = _load_stock_df(symbol, range_key)
    if df.empty:
        return jsonify({"error": f"{symbol} 无数据"}), 404

    # 填充默认参数 + 执行策略
    params = _fill_default_params(strategy["config"], user_params)
    signals = strategy["fn"](df.copy(), params)

    return jsonify(signals)


@strategy_bp.route("/api/strategy_backtest_combined", methods=["POST"])
def strategy_backtest_combined():
    """
    策略信号 + 回测，保持旧接口兼容。

    请求体:
    {
        "ticker": "AAPL",
        "range": "2y",
        "strategy": "ma_cross",
        "params": {"short_ma": 50, "long_ma": 200},
        "mode": "both",          // "signal" / "backtest" / "both"
        "initial_capital": 10000,
        "commission": 0.001
    }
    """
    try:
        payload = request.get_json()
        symbol = payload.get("ticker", "AAPL").upper()
        range_key = payload.get("range", "6mo")
        strategy_name = payload.get("strategy", "ma_cross")
        mode = payload.get("mode", "both")
        user_params = payload.get("params", {})
        initial_capital = payload.get("initial_capital", 10000)
        commission = payload.get("commission", 0.001)

        # 查找策略
        strategy = get_strategy(strategy_name)
        if strategy is None:
            available = [s["name"] for s in list_strategies()]
            return jsonify({
                "error": f"未知策略: {strategy_name}",
                "available": available,
            }), 400

        # 加载数据
        df = _load_stock_df(symbol, range_key)
        if df.empty:
            return jsonify({"error": f"{symbol} 在 {range_key} 范围内无数据"}), 404

        params = _fill_default_params(strategy["config"], user_params)

        result = {
            "symbol": symbol,
            "strategy": strategy_name,
            "range": range_key,
            "params": params,
            "mode": mode,
        }

        # 信号
        signals = []
        if mode in ("signal", "both"):
            try:
                signals = strategy["fn"](df.copy(), params)
                result["signals"] = signals
            except Exception as e:
                result["signals"] = []
                result["signals_error"] = f"信号生成失败: {str(e)}"

        # 回测
        if mode in ("backtest", "both"):
            try:
                if not signals:
                    signals = strategy["fn"](df.copy(), params)
                backtest_result = run_backtest(
                    df.copy(), signals,
                    initial_capital=initial_capital,
                    commission=commission,
                )
                result["backtest"] = backtest_result
            except Exception as e:
                result["backtest"] = {}
                result["backtest_error"] = f"回测失败: {str(e)}"

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500