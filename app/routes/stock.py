from flask import Blueprint, request, jsonify
from app.services.stock_service import get_stock_series
from app.services.stock_service import get_stock_series, is_local_fresh

stock_bp = Blueprint("stock", __name__)

def _parse_policy(autoupdate_param: str) -> str:
    """
    '0' -> local   仅本地
    '1'/None -> auto  默认
    'force' -> force  跳过防抖
    其他非法值按 auto 处理
    """
    v = (autoupdate_param or "1").strip().lower()
    if v == "0":
        return "local"
    if v == "force":
        return "force"
    return "auto"

@stock_bp.route("/api/stock", methods=["GET"])
def get_stock_data():
    symbol    = request.args.get("ticker", "AAPL").upper()
    range_key = request.args.get("range",  "6mo")
    ma_param  = request.args.get("ma",     "")

    policy = "local"  # 只读本地

    ma_values = [int(x) for x in ma_param.split(",") if x.strip().isdigit()]
    data, freshness = get_stock_series(symbol, range_key, ma_values, policy=policy)

    # 新鲜度
    fresh_info = is_local_fresh(symbol)
    is_latest = bool(fresh_info.get("fresh", False))
    need_update = not is_latest

    payload = {
        "data": data,
        "meta": {
            "symbol": symbol,
            "range": range_key,
            "ma": ma_values,
            "status": freshness.get("status", ""),
            "note": freshness.get("message", ""),
            "is_latest": is_latest,
            "need_update": need_update,
            "last_local": fresh_info.get("last_local"),
            "expected_last": fresh_info.get("expected_last"),
        }
    }

    resp = jsonify(payload)

    # 响应头依然保留，兼容旧逻辑
    resp.headers["X-Stock-Status"] = freshness.get("status", "")
    note = freshness.get("message", "")
    if note:
        resp.headers["X-Stock-Note"] = note
    resp.headers["X-Stock-Is-Latest"] = "true" if is_latest else "false"
    resp.headers["X-Stock-Need-Update"] = "true" if need_update else "false"

    return resp

