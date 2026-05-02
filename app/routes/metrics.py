# app/routes/metrics.py
"""
GET /api/stock_metrics?ticker=AAPL&range=2y

返回个股在指定时间范围内的风险收益指标，无需跑策略或回测。
支持与 ^GSPC 对比计算 Beta。
"""

from flask import Blueprint, request, jsonify
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json, os

from app.config import get_data_path, DATE_RANGE_OFFSET

metrics_bp = Blueprint("metrics", __name__)


def _load_df(symbol: str, range_key: str) -> pd.DataFrame:
    """加载数据并按时间范围裁剪。"""
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


def _calc_metrics(df: pd.DataFrame, benchmark_df: pd.DataFrame = None,
                  risk_free_rate: float = 0.02) -> dict:
    """
    计算全部指标。

    参数:
        df:             个股 DataFrame，含 date, close
        benchmark_df:   基准 DataFrame（^GSPC），含 date, close，可为 None
        risk_free_rate: 年化无风险利率

    返回:
        指标字典
    """
    if len(df) < 2:
        return {"error": "数据不足，无法计算指标"}

    close = df["close"].astype(float)
    daily_returns = close.pct_change().dropna()

    days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
    trading_days = len(daily_returns)

    if trading_days < 2 or days <= 0:
        return {"error": "数据不足，无法计算指标"}

    # ===== 收益类 =====
    total_return = (close.iloc[-1] / close.iloc[0] - 1) * 100
    annual_return = ((close.iloc[-1] / close.iloc[0]) ** (365 / days) - 1) * 100

    # 近一年收益（取最近252个交易日）
    if trading_days >= 252:
        ytd_return = (close.iloc[-1] / close.iloc[-252] - 1) * 100
    else:
        ytd_return = total_return  # 数据不足一年就用全部

    # ===== 风险类 =====
    annual_volatility = daily_returns.std() * np.sqrt(252) * 100

    # 最大回撤
    cumulative = (1 + daily_returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative / running_max - 1) * 100
    max_drawdown = drawdown.min()

    # 最大回撤持续天数（从峰值到恢复）
    dd_dates = df["date"].iloc[1:]  # 与 daily_returns 对齐
    dd_dates = dd_dates.reset_index(drop=True)
    drawdown = drawdown.reset_index(drop=True)

    max_dd_duration = 0
    current_dd_start = None
    for i in range(len(drawdown)):
        if drawdown.iloc[i] < 0:
            if current_dd_start is None:
                current_dd_start = dd_dates.iloc[i]
        else:
            if current_dd_start is not None:
                duration = (dd_dates.iloc[i] - current_dd_start).days
                max_dd_duration = max(max_dd_duration, duration)
                current_dd_start = None
    # 如果最后仍在回撤中
    if current_dd_start is not None:
        duration = (dd_dates.iloc[-1] - current_dd_start).days
        max_dd_duration = max(max_dd_duration, duration)

    # ===== 风险调整收益类 =====
    annual_return_decimal = annual_return / 100
    annual_vol_decimal = annual_volatility / 100

    # 夏普比率
    if annual_vol_decimal > 0:
        sharpe_ratio = (annual_return_decimal - risk_free_rate) / annual_vol_decimal
    else:
        sharpe_ratio = 0

    # 索提诺比率（只用下行波动率）
    downside_returns = daily_returns[daily_returns < 0]
    if len(downside_returns) > 0:
        downside_std = downside_returns.std() * np.sqrt(252)
        sortino_ratio = (annual_return_decimal - risk_free_rate) / downside_std if downside_std > 0 else 0
    else:
        sortino_ratio = 0

    # Calmar 比率（年化收益 / 最大回撤绝对值）
    if max_drawdown < 0:
        calmar_ratio = annual_return_decimal / abs(max_drawdown / 100)
    else:
        calmar_ratio = 0

    # ===== Beta（相对基准）=====
    beta = None
    if benchmark_df is not None and len(benchmark_df) >= 2:
        # 按日期对齐
        bench = benchmark_df[["date", "close"]].copy()
        bench.columns = ["date", "bench_close"]
        merged = pd.merge(
            df[["date", "close"]], bench,
            on="date", how="inner"
        ).sort_values("date")

        if len(merged) >= 20:
            stock_ret = merged["close"].astype(float).pct_change().dropna()
            bench_ret = merged["bench_close"].astype(float).pct_change().dropna()

            # 确保长度一致
            min_len = min(len(stock_ret), len(bench_ret))
            stock_ret = stock_ret.iloc[:min_len]
            bench_ret = bench_ret.iloc[:min_len]

            cov = np.cov(stock_ret, bench_ret)
            if cov[1][1] > 0:
                beta = round(cov[0][1] / cov[1][1], 3)

    result = {
        # 收益类
        "total_return": round(total_return, 2),
        "annual_return": round(annual_return, 2),
        "recent_1y_return": round(ytd_return, 2),

        # 风险类
        "annual_volatility": round(annual_volatility, 2),
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_duration_days": max_dd_duration,

        # 风险调整收益类
        "sharpe_ratio": round(sharpe_ratio, 3),
        "sortino_ratio": round(sortino_ratio, 3),
        "calmar_ratio": round(calmar_ratio, 3),

        # 其他
        "trading_days": trading_days,
        "calendar_days": days,
        "start_date": str(df["date"].iloc[0].date()),
        "end_date": str(df["date"].iloc[-1].date()),
        "start_price": round(float(close.iloc[0]), 2),
        "end_price": round(float(close.iloc[-1]), 2),
    }

    if beta is not None:
        result["beta"] = beta

    return result


@metrics_bp.route("/api/stock_metrics", methods=["GET"])
def stock_metrics():
    """
    GET /api/stock_metrics?ticker=AAPL&range=2y

    返回个股风险收益指标。
    """
    ticker = request.args.get("ticker", "AAPL").upper()
    range_key = request.args.get("range", "2y")

    # 加载个股数据
    df = _load_df(ticker, range_key)
    if df.empty:
        return jsonify({"error": f"{ticker} 在 {range_key} 范围内无数据"}), 404

    # 加载基准（^GSPC）用于计算 Beta
    benchmark_df = None
    if ticker != "^GSPC":
        benchmark_df = _load_df("^GSPC", range_key)
        if benchmark_df.empty:
            benchmark_df = None

    metrics = _calc_metrics(df, benchmark_df)

    return jsonify({
        "ticker": ticker,
        "range": range_key,
        "metrics": metrics,
    })