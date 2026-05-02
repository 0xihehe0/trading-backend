'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:23:46
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2026-05-01 22:06:18
FilePath: \backend\app\__init__.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Flask
from flask_cors import CORS
from app.routes.strategy import strategy_bp
from app.routes.stock import stock_bp
from app.routes.symbols import symbols_bp
from app.routes.backtest import backtest_bp
from app.routes.newstock import newstock_bp
from app.routes.metrics import metrics_bp

# flask --app app:create_app --debug run
# 0L0HYADURTF4E1DI

def create_app():
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(strategy_bp)
    app.register_blueprint(stock_bp)
    app.register_blueprint(symbols_bp)
    app.register_blueprint(backtest_bp)
    app.register_blueprint(newstock_bp)
    app.register_blueprint(metrics_bp)
    return app
