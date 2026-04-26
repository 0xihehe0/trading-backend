# app/services/backtest_engine.py
"""
通用回测引擎。

只做一件事：接收 DataFrame + 信号列表，模拟交易，计算收益指标。
不关心信号是哪个策略产出的。
"""

import pandas as pd
import numpy as np


def run_backtest(df: pd.DataFrame, signals: list[dict],
                 initial_capital: float = 10000,
                 commission: float = 0.001) -> dict:
    """
    通用回测。

    参数:
        df:       包含 date(datetime), close 列的 DataFrame
        signals:  [{"type":"buy"/"sell", "date":"YYYY-MM-DD", "price": float}, ...]
        initial_capital: 初始资金
        commission: 手续费率

    返回:
        {summary, equity_curve, trades, metrics}
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # 把信号转成按日期查找的 dict
    signal_map = {}
    for s in signals:
        signal_map[s["date"]] = s["type"]

    # 添加信号列
    df["signal"] = df["date"].apply(
        lambda d: 1 if signal_map.get(str(d.date())) == "buy"
        else (-1 if signal_map.get(str(d.date())) == "sell" else 0)
    )

    # ===== 模拟交易 =====
    capital = initial_capital
    shares = 0
    trades = []

    for _, row in df.iterrows():
        if row["signal"] == 1 and shares == 0:
            # 全仓买入
            shares = int(capital / (row["close"] * (1 + commission)))
            if shares <= 0:
                continue
            cost = shares * row["close"] * (1 + commission)
            capital -= cost
            position_value = shares * row["close"]

            trades.append({
                "type": "buy",
                "date": str(row["date"].date()),
                "price": round(row["close"], 2),
                "shares": shares,
                "cost": round(cost, 2),
                "capital": round(capital, 2),
                "portfolio": round(capital + position_value, 2),
            })

        elif row["signal"] == -1 and shares > 0:
            # 全部卖出
            proceeds = shares * row["close"] * (1 - commission)
            capital += proceeds

            trades.append({
                "type": "sell",
                "date": str(row["date"].date()),
                "price": round(row["close"], 2),
                "shares": shares,
                "proceeds": round(proceeds, 2),
                "capital": round(capital, 2),
                "portfolio": round(capital, 2),
            })

            shares = 0

    # ===== 计算每日组合价值 =====
    daily_capital = initial_capital
    daily_shares = 0
    portfolio_values = []

    for _, row in df.iterrows():
        if row["signal"] == 1 and daily_shares == 0:
            daily_shares = int(daily_capital / (row["close"] * (1 + commission)))
            if daily_shares > 0:
                daily_capital -= daily_shares * row["close"] * (1 + commission)
        elif row["signal"] == -1 and daily_shares > 0:
            daily_capital += daily_shares * row["close"] * (1 - commission)
            daily_shares = 0

        pv = daily_capital + daily_shares * row["close"]
        portfolio_values.append({
            "date": str(row["date"].date()),
            "portfolio_value": round(pv, 2),
            "position_value": round(daily_shares * row["close"], 2),
            "cash": round(daily_capital, 2),
        })

    df["portfolio_value"] = [p["portfolio_value"] for p in portfolio_values]
    df["returns"] = df["portfolio_value"].pct_change()

    # ===== 最终资产（未平仓则按最后收盘价清算）=====
    if shares > 0:
        final_value = capital + shares * df["close"].iloc[-1] * (1 - commission)
    else:
        final_value = capital

    # ===== 计算指标 =====
    total_return = (final_value / initial_capital - 1) * 100

    days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
    annual_return = ((final_value / initial_capital) ** (365 / days) - 1) if days > 0 else 0
    annual_return_pct = annual_return * 100

    # 夏普比率
    risk_free_rate = 0.02
    if len(df) > 1 and df["returns"].std() > 0:
        returns_std = df["returns"].std() * np.sqrt(252)
        sharpe_ratio = (annual_return - risk_free_rate) / returns_std
    else:
        sharpe_ratio = 0

    # 最大回撤
    cumulative = (1 + df["returns"].fillna(0)).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative / running_max - 1) * 100
    max_drawdown = drawdown.min()

    # 胜率（买卖配对计算）
    win_count = 0
    loss_count = 0
    for i in range(0, len(trades) - 1, 2):
        if i + 1 < len(trades):
            if trades[i + 1]["portfolio"] > trades[i]["portfolio"]:
                win_count += 1
            else:
                loss_count += 1

    total_trades = win_count + loss_count
    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

    # ===== 组装结果 =====
    metrics = {
        "total_return": round(total_return, 2),
        "annual_return": round(annual_return_pct, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "max_drawdown": round(max_drawdown, 2),
        "win_rate": round(win_rate, 2),
        "trade_count": len(trades),
        "win_count": win_count,
        "loss_count": loss_count,
        "days": days,
    }

    summary = {
        "period": f"{df['date'].iloc[0].date()} - {df['date'].iloc[-1].date()}",
        "initial_capital": initial_capital,
        "final_capital": round(final_value, 2),
        "profit": round(final_value - initial_capital, 2),
    }

    return {
        "summary": summary,
        "equity_curve": portfolio_values,
        "trades": trades,
        "metrics": metrics,
    }