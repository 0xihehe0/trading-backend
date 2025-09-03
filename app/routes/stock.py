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
    symbol     = request.args.get("ticker", "AAPL").upper()
    range_key  = request.args.get("range",  "6mo")
    ma_param   = request.args.get("ma",     "")
    with_meta  = request.args.get("with_meta", "0") == "1"

    # 当前阶段：只读本地
    policy = "local"

    ma_values = [int(x) for x in ma_param.split(",") if x.strip().isdigit()]
    data, freshness = get_stock_series(symbol, range_key, ma_values, policy=policy)

    # 新增：计算是否为最新
    fresh_info = is_local_fresh(symbol)
    is_latest = bool(fresh_info.get("fresh", False))
    need_update = not is_latest

    # 旧兼容：放在响应头里
    resp_headers = {
        "X-Stock-Status": freshness.get("status", ""),
        "X-Stock-Note": freshness.get("message", ""),
        "X-Stock-Is-Latest": "true" if is_latest else "false",
        "X-Stock-Need-Update": "true" if need_update else "false",
    }

    if with_meta:
        # 新返回：带 meta，不破坏老接口（只有 with_meta=1 才启用）
        payload = {
            "data": data,
            "meta": {
                "symbol": symbol,
                "is_latest": is_latest,             # ✅ 是否已是最新
                "need_update": need_update,         # ✅ 是否需要更新
                "last_local": fresh_info.get("last_local"),
                "expected_last": fresh_info.get("expected_last"),
                "status": freshness.get("status", ""),
                "note": freshness.get("message", ""),
                "range": range_key,
                "ma": ma_values,
            }
        }
        resp = jsonify(payload)
    else:
        # 旧行为：只返回 list
        resp = jsonify(data)

    # 统一加响应头，兼容老前端
    for k, v in resp_headers.items():
        if v:
            resp.headers[k] = v
    return resp

