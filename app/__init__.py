'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:23:46
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-09 21:49:27
FilePath: \backend\app\__init__.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from flask import Flask
from flask_cors import CORS
from app.routes.strategy import strategy_bp
from app.routes.stock import stock_bp  # 如果你有的话

def create_app():
    app = Flask(__name__)
    CORS(app)
    
    app.register_blueprint(strategy_bp)
    app.register_blueprint(stock_bp)

    return app
