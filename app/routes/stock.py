'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:24:42
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-08-19 21:49:16
FilePath: \backend\app\routes\stock.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
# app/routes/stock.py
"""
职责：解析参数 → 调用 service → 返回 JSON
路径：/api/stock
"""
from flask import Blueprint, request, jsonify
from app.services.stock_service import get_stock_series

stock_bp = Blueprint("stock", __name__)

@stock_bp.route("/api/stock", methods=["GET"])
def get_stock_data():
    symbol     = request.args.get("ticker", "AAPL").upper()
    range_key  = request.args.get("range", "6mo")
    ma_param   = request.args.get("ma", "")
    autoupdate = request.args.get("autoupdate", "0")  # ⚠️ 默认关闭自动更新
    do_update  = (autoupdate != "0")

    ma_values = [int(x) for x in ma_param.split(",") if x.strip().isdigit()]

    result, freshness = get_stock_series(symbol, range_key, ma_values, autoupdate=do_update)
    if result is None:
        return jsonify(freshness), 404  # freshness 里就是 {"error": "..."} 结构

    # 你也可以把 freshness 放到 header 里，便于前端调试
    # resp = jsonify(result)
    # resp.headers["X-Freshness"] = freshness.get("message", "")
    # return resp

    # 调试打印
    print("[route] freshness:", freshness, flush=True)
    return jsonify(result)
