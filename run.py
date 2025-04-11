'''
Author: yaojinxi 864554492@qq.com
Date: 2025-04-08 21:21:48
LastEditors: yaojinxi 864554492@qq.com
LastEditTime: 2025-04-11 22:26:52
FilePath: \backend\run.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
from app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(port=5000, debug=True)