import pandas as pd
import numpy as np

def backtest_ma_cross_strategy(df, initial_capital=10000, short=50, long=200, commission=0.001):
    """
    均线交叉策略回测
    
    参数:
    df: 包含OHLC数据的DataFrame
    initial_capital: 初始资金
    short: 短期均线周期
    long: 长期均线周期
    commission: 交易手续费率
    
    返回:
    包含回测结果的字典
    """
    # 复制数据，避免修改原始数据
    backtest_df = df.copy()
    
    # 计算均线
    backtest_df['ma_short'] = backtest_df['close'].rolling(short).mean()
    backtest_df['ma_long'] = backtest_df['close'].rolling(long).mean()
    
    # 去除NaN值
    backtest_df.dropna(inplace=True)
    
    # 创建信号列
    backtest_df['signal'] = 0  # 0:无信号, 1:买入, -1:卖出
    
    # 识别交叉信号
    for i in range(1, len(backtest_df)):
        prev = backtest_df.iloc[i - 1]
        curr = backtest_df.iloc[i]
        
        if prev['ma_short'] < prev['ma_long'] and curr['ma_short'] > curr['ma_long']:
            backtest_df.loc[backtest_df.index[i], 'signal'] = 1  # 买入信号
        elif prev['ma_short'] > prev['ma_long'] and curr['ma_short'] < curr['ma_long']:
            backtest_df.loc[backtest_df.index[i], 'signal'] = -1  # 卖出信号
    
    # 模拟交易过程
    positions = pd.DataFrame(index=backtest_df.index).fillna(0.0)
    positions['stock'] = 0  # 持仓数量
    
    capital = initial_capital
    position_value = 0
    shares = 0
    trades = []
    
    for i, row in backtest_df.iterrows():
        if row['signal'] == 1 and shares == 0:  # 买入信号且当前无持仓
            # 全仓买入
            shares = int(capital / (row['close'] * (1 + commission)))
            cost = shares * row['close'] * (1 + commission)
            capital -= cost
            position_value = shares * row['close']
            positions.loc[i, 'stock'] = shares
            
            trades.append({
                'type': 'buy',
                'date': str(row['date'].date()),
                'price': round(row['close'], 2),
                'shares': shares,
                'cost': round(cost, 2),
                'capital': round(capital, 2),
                'portfolio': round(capital + position_value, 2)
            })
            
        elif row['signal'] == -1 and shares > 0:  # 卖出信号且有持仓
            # 全部卖出
            proceeds = shares * row['close'] * (1 - commission)
            capital += proceeds
            positions.loc[i, 'stock'] = 0
            
            trades.append({
                'type': 'sell',
                'date': str(row['date'].date()),
                'price': round(row['close'], 2),
                'shares': shares,
                'proceeds': round(proceeds, 2),
                'capital': round(capital, 2),
                'portfolio': round(capital, 2)
            })
            
            shares = 0
            position_value = 0
    
    # 计算每日资产价值
    backtest_df['position'] = positions['stock']
    backtest_df['position_value'] = backtest_df['position'] * backtest_df['close']
    backtest_df['cash'] = initial_capital  # 初始化现金列
    
    # 更新现金和组合价值
    for i in range(len(backtest_df)):
        if i > 0:
            backtest_df.loc[backtest_df.index[i], 'cash'] = backtest_df.loc[backtest_df.index[i-1], 'cash']
        
        if backtest_df.loc[backtest_df.index[i], 'signal'] == 1:  # 买入
            backtest_df.loc[backtest_df.index[i], 'cash'] -= backtest_df.loc[backtest_df.index[i], 'position_value'] * (1 + commission)
        elif backtest_df.loc[backtest_df.index[i], 'signal'] == -1:  # 卖出
            backtest_df.loc[backtest_df.index[i], 'cash'] += backtest_df.loc[backtest_df.index[i-1], 'position_value'] * (1 - commission)
    
    backtest_df['portfolio_value'] = backtest_df['cash'] + backtest_df['position_value']
    backtest_df['returns'] = backtest_df['portfolio_value'].pct_change()
    
    # 假设最后一天结束时清仓平仓
    if shares > 0:
        last_price = backtest_df['close'].iloc[-1]
        proceeds = shares * last_price * (1 - commission)
        capital += proceeds
        final_portfolio_value = capital
    else:
        final_portfolio_value = backtest_df['portfolio_value'].iloc[-1]
    
    # 计算回测指标
    total_return = (final_portfolio_value / initial_capital - 1) * 100
    
    # 计算年化收益率
    days = (backtest_df['date'].iloc[-1] - backtest_df['date'].iloc[0]).days
    annual_return = (final_portfolio_value / initial_capital) ** (365 / days) - 1 if days > 0 else 0
    annual_return_pct = annual_return * 100
    
    # 计算夏普比率
    risk_free_rate = 0.02  # 假设无风险利率为2%
    if len(backtest_df) > 1:
        returns_std = backtest_df['returns'].std() * np.sqrt(252)  # 年化标准差
        sharpe_ratio = (annual_return - risk_free_rate) / returns_std if returns_std > 0 else 0
    else:
        sharpe_ratio = 0
    
    # 计算最大回撤
    backtest_df['cumulative_return'] = (1 + backtest_df['returns']).cumprod()
    backtest_df['running_max'] = backtest_df['cumulative_return'].cummax()
    backtest_df['drawdown'] = (backtest_df['cumulative_return'] / backtest_df['running_max'] - 1) * 100
    max_drawdown = backtest_df['drawdown'].min()
    
    # 计算胜率
    win_count = 0
    loss_count = 0
    for i in range(0, len(trades), 2):
        if i+1 < len(trades):
            if trades[i+1]['portfolio'] > trades[i]['portfolio']:
                win_count += 1
            else:
                loss_count += 1
    
    win_rate = win_count / (win_count + loss_count) * 100 if (win_count + loss_count) > 0 else 0
    
    # 准备资金曲线数据
    equity_curve = [
        {
            'date': str(row['date'].date()),
            'portfolio_value': round(row['portfolio_value'], 2),
            'position_value': round(row['position_value'], 2),
            'cash': round(row['cash'], 2)
        } for _, row in backtest_df.iterrows()
    ]
    
    # 准备回测结果摘要
    metrics = {
        'total_return': round(total_return, 2),
        'annual_return': round(annual_return_pct, 2),
        'sharpe_ratio': round(sharpe_ratio, 2),
        'max_drawdown': round(max_drawdown, 2),
        'win_rate': round(win_rate, 2),
        'trade_count': len(trades),
        'win_count': win_count,
        'loss_count': loss_count,
        'days': days
    }
    
    summary = {
        'symbol': df['date'].iloc[0],
        'period': f"{df['date'].iloc[0].date()} - {df['date'].iloc[-1].date()}",
        'initial_capital': initial_capital,
        'final_capital': round(final_portfolio_value, 2),
        'profit': round(final_portfolio_value - initial_capital, 2),
        'strategy_params': {
            'short_ma': short,
            'long_ma': long,
            'commission': commission
        }
    }
    
    # 返回结果
    return {
        'summary': summary,
        'equity_curve': equity_curve,
        'trades': trades,
        'metrics': metrics
    }