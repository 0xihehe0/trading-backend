
from flask import Blueprint, jsonify
import pandas as pd

symbols_bp = Blueprint('symbols', __name__)

@symbols_bp.route('/api/symbols', methods=['GET'])
def get_symbols():
    try:
        # 读取 CSV，指定 utf-8 编码更稳健
        df = pd.read_csv('data/sp500_symbols.csv', encoding='utf-8')

        # 清洗字段名以防有空格或特殊字符
        df.columns = [col.strip() for col in df.columns]

        if 'Symbol' not in df.columns or 'Security' not in df.columns:
            raise ValueError("CSV 中缺少 'Symbol' 或 'Security' 列")

        df = df.dropna(subset=['Symbol', 'Security'])

        results = [
            {"label": f"{row['Security']} ({row['Symbol']})", "value": row['Symbol']}
            for _, row in df.iterrows()
        ]

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": f"加载 symbols 失败: {str(e)}"}), 500
