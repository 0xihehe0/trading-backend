import yfinance as yf
import pandas as pd
import json
import time
from datetime import datetime
import os

# 🔁 替换为你下载好的成分股列表
CSV_PATH = "data/sp500_symbols.csv"  # 包含 symbol 列
OUTPUT_DIR = "data"

def ensure_directory_exists(directory):
    """确保输出目录存在"""
    if not os.path.exists(directory):
        os.makedirs(directory)

def fetch_sp500_symbols_from_wiki():
    """从维基百科获取S&P500成分股列表"""
    print("📊 从维基百科获取S&P500成分股列表...")
    
    # 1. 从Wikipedia抓取所有HTML表格
    tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    
    # 2. 第一个表格就是成分股列表
    sp500_df = tables[0]
    
    # 3. 只取我们需要的两列：代码 & 公司名称
    sp500_df = sp500_df[['Symbol', 'Security']]
    
    # 4. 去掉可能存在的空格或特殊字符
    sp500_df['Symbol'] = sp500_df['Symbol'].str.replace('.', '-', regex=False).str.strip()
    
    # 5. 保存为CSV文件（供抓历史数据使用）
    ensure_directory_exists(OUTPUT_DIR)
    csv_path = os.path.join(OUTPUT_DIR, "sp500_symbols.csv")
    sp500_df.to_csv(csv_path, index=False)
    
    print(f"✅ 已保存成分股列表到 {csv_path}")
    return sp500_df['Symbol'].dropna().unique().tolist()

def get_fundamentals(ticker):
    """获取股票的基本面数据"""
    try:
        info = ticker.info
        fundamentals = {
            # 估值指标
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "price_to_sales": info.get("priceToSalesTrailing12Months"),
            "price_to_book": info.get("priceToBook"),
            "enterprise_value": info.get("enterpriseValue"),
            "enterprise_to_revenue": info.get("enterpriseToRevenue"),
            "enterprise_to_ebitda": info.get("enterpriseToEbitda"),
            
            # 公司基本信息
            "market_cap": info.get("marketCap"),
            "company_name": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "website": info.get("website"),
            "business_summary": info.get("longBusinessSummary"),
            
            # 财务数据
            "revenue": info.get("totalRevenue"),
            "gross_profit": info.get("grossProfits"),
            "ebitda": info.get("ebitda"),
            "net_income": info.get("netIncomeToCommon"),
            
            # 股息数据
            "dividend_rate": info.get("dividendRate"),
            "dividend_yield": info.get("dividendYield"),
            "payout_ratio": info.get("payoutRatio"),
            
            # 成长数据
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            
            # 财务健康
            "debt_to_equity": info.get("debtToEquity"),
            "current_ratio": info.get("currentRatio"),
            "return_on_equity": info.get("returnOnEquity"),
            "return_on_assets": info.get("returnOnAssets")
        }
        return fundamentals
    except Exception as e:
        print(f"  ⚠️ 无法获取基本面数据: {e}")
        return {}

def get_financial_statements(ticker):
    """获取财务报表数据"""
    try:
        financials = {
            "income_statement": ticker.income_stmt.to_dict() if hasattr(ticker, 'income_stmt') and ticker.income_stmt is not None else {},
            "balance_sheet": ticker.balance_sheet.to_dict() if hasattr(ticker, 'balance_sheet') and ticker.balance_sheet is not None else {},
            "cash_flow": ticker.cashflow.to_dict() if hasattr(ticker, 'cashflow') and ticker.cashflow is not None else {}
        }
        
        # 将日期索引转换为字符串
        for statement_type, statement_data in financials.items():
            if statement_data:
                # 转换复杂的嵌套字典为可序列化格式
                serializable_data = {}
                for date, metrics in statement_data.items():
                    date_str = str(date)
                    serializable_data[date_str] = {}
                    for metric, value in metrics.items():
                        serializable_data[date_str][metric] = float(value) if pd.notnull(value) else None
                financials[statement_type] = serializable_data
                
        return financials
    except Exception as e:
        print(f"  ⚠️ 无法获取财务报表: {e}")
        return {
            "income_statement": {},
            "balance_sheet": {},
            "cash_flow": {}
        }

def get_analyst_data(ticker):
    """获取分析师预测和目标价数据"""
    try:
        recommendations = ticker.recommendations
        rec_dict = {}

        if recommendations is not None and not recommendations.empty:
            for index, row in recommendations.iterrows():
                try:
                    # 安全转换日期
                    date_str = str(index.date()) if hasattr(index, 'date') else str(index)
                    rec_dict[date_str] = {
                        "firm": row.get("Firm", ""),
                        "to_grade": row.get("To Grade", ""),
                        "from_grade": row.get("From Grade", ""),
                        "action": row.get("Action", "")
                    }
                except Exception as e:
                    print(f"    ⚠️ 跳过无效推荐数据: {e}")
                    continue

            return {
                "recommendations": rec_dict,
                "target_price": ticker.info.get("targetMeanPrice"),
                "target_high": ticker.info.get("targetHighPrice"),
                "target_low": ticker.info.get("targetLowPrice")
            }

        return {}

    except Exception as e:
        print(f"  ⚠️ 无法获取分析师数据: {e}")
        return {}


def fetch_and_save():
    """获取并保存S&P500股票的历史价格和基本面数据"""
    ensure_directory_exists(OUTPUT_DIR)
    
    # 检查CSV文件是否存在，如果不存在则从维基百科获取
    if not os.path.exists(CSV_PATH):
        symbols = fetch_sp500_symbols_from_wiki()
    else:
        df_symbols = pd.read_csv(CSV_PATH)
        symbols = df_symbols['Symbol'].dropna().unique().tolist()

    price_data = {}  # 历史价格数据
    fundamental_data = {}  # 基本面数据
    financial_statements = {}  # 财务报表
    analyst_data = {}  # 分析师数据
    
    total_symbols = len(symbols)
    success_count = 0
    fail_count = 0
    
    print(f"🚀 开始获取{total_symbols}只股票的数据...")
    
    for i, symbol in enumerate(symbols):
        print(f"[{i+1}/{total_symbols}] 处理: {symbol}")
        try:
            ticker = yf.Ticker(symbol)
            
            # 1. 获取历史价格数据
            print(f"  📈 获取{symbol}的历史价格...")
            df = ticker.history(period='max', interval='1d')
            if not df.empty:
                df.reset_index(inplace=True)
                price_data[symbol] = [
                    {"date": str(row['Date'].date()), 
                     "open": round(row['Open'], 2) if not pd.isna(row['Open']) else None,
                     "high": round(row['High'], 2) if not pd.isna(row['High']) else None,
                     "low": round(row['Low'], 2) if not pd.isna(row['Low']) else None,
                     "close": round(row['Close'], 2) if not pd.isna(row['Close']) else None,
                     "volume": int(row['Volume']) if not pd.isna(row['Volume']) else None}
                    for _, row in df.iterrows()
                ]
            
            # 2. 获取基本面数据
            print(f"  📊 获取{symbol}的基本面数据...")
            fundamental_data[symbol] = get_fundamentals(ticker)
            
            # 3. 获取财务报表
            print(f"  📑 获取{symbol}的财务报表...")
            financial_statements[symbol] = get_financial_statements(ticker)
            
            # 4. 获取分析师数据
            print(f"  👨‍💼 获取{symbol}的分析师预测...")
            analyst_data[symbol] = get_analyst_data(ticker)
            
            success_count += 1
            # 每处理一个请求暂停一段时间，避免被限制
            time.sleep(1.5)
            
        except Exception as e:
            print(f"❌ 获取{symbol}数据失败: {e}")
            fail_count += 1
            time.sleep(1)  # 发生错误后短暂暂停

    # 保存各类数据到不同文件
    data_files = {
        "price_data": os.path.join(OUTPUT_DIR, "sp500_prices.json"),
        "fundamental_data": os.path.join(OUTPUT_DIR, "sp500_fundamentals.json"),
        "financial_statements": os.path.join(OUTPUT_DIR, "sp500_financials.json"),
        "analyst_data": os.path.join(OUTPUT_DIR, "sp500_analyst.json")
    }
    
    for data_type, file_path in data_files.items():
        with open(file_path, "w") as f:
            json.dump(eval(data_type), f)
        print(f"✅ 已保存{data_type}到 {file_path}")
    
    # 保存完整数据（可选，数据量可能很大）
    all_data = {
        "price_data": price_data,
        "fundamental_data": fundamental_data,
        "financial_statements": financial_statements,
        "analyst_data": analyst_data,
        "metadata": {
            "update_time": str(datetime.now()),
            "total_symbols": total_symbols,
            "success_count": success_count,
            "fail_count": fail_count
        }
    }
    
    with open(os.path.join(OUTPUT_DIR, "sp500_all_data.json"), "w") as f:
        json.dump(all_data, f)
    
    print(f"✅ 数据更新完成")
    print(f"🎯 成功获取: {success_count}/{total_symbols} 股票数据")
    print(f"⛔ 失败: {fail_count}/{total_symbols}")
    print(f"🕒 更新时间: {datetime.now()}")
    
    return all_data

if __name__ == "__main__":
    fetch_and_save()