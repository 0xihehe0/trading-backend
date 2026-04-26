# app/strategies/rsi.py
"""
RSI 相对强弱指标策略

RSI 低于超卖线买入，高于超买线卖出。
"""
import pandas as pd

CONFIG = {
    "name": "rsi",
    "label": "RSI 相对强弱策略",
    "description": "RSI 跌破超卖线时买入，突破超买线时卖出。适合震荡市中捕捉超跌反弹。",
    "params": [
        {
            "key": "period",
            "label": "RSI 周期",
            "type": "int",
            "default": 14,
            "min": 5,
            "max": 50,
        },
        {
            "key": "oversold",
            "label": "超卖线",
            "type": "int",
            "default": 30,
            "min": 10,
            "max": 40,
        },
        {
            "key": "overbought",
            "label": "超买线",
            "type": "int",
            "default": 70,
            "min": 60,
            "max": 90,
        },
    ],
}


def _calc_rsi(series: pd.Series, period: int) -> pd.Series:
    """计算 RSI。"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def generate_signals(df: pd.DataFrame, params: dict) -> list[dict]:
    """
    统一信号接口。

    逻辑：
        - RSI 从超卖线下方穿越到上方 → 买入（超跌反弹）
        - RSI 从超买线下方穿越到上方 → 卖出（过热回调）

    参数:
        df: 必须包含 'date'(datetime) 和 'close' 列
        params: {"period": int, "oversold": int, "overbought": int}

    返回:
        [{"type": "buy"/"sell", "date": "YYYY-MM-DD", "price": float}, ...]
    """
    period = params.get("period", CONFIG["params"][0]["default"])
    oversold = params.get("oversold", CONFIG["params"][1]["default"])
    overbought = params.get("overbought", CONFIG["params"][2]["default"])

    df = df.copy()
    df["rsi"] = _calc_rsi(df["close"], period)
    df.dropna(inplace=True)

    if len(df) < 2:
        return []

    signals = []
    for i in range(1, len(df)):
        prev_rsi = df.iloc[i - 1]["rsi"]
        curr_rsi = df.iloc[i]["rsi"]
        curr = df.iloc[i]

        # 从超卖区上穿 → 买入
        if prev_rsi < oversold and curr_rsi >= oversold:
            signals.append({
                "type": "buy",
                "date": str(curr["date"].date()),
                "price": round(curr["close"], 2),
            })

        # 从超买区下穿 → 卖出
        elif prev_rsi > overbought and curr_rsi <= overbought:
            signals.append({
                "type": "sell",
                "date": str(curr["date"].date()),
                "price": round(curr["close"], 2),
            })

    return signals