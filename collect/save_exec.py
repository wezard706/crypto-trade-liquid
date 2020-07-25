import logging
import logging.handlers

# logger
logger = logging.getLogger('crypto')
logger.setLevel(logging.DEBUG)
# 標準出力
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s, %(message)s'))
logger.addHandler(stream_handler)
# ファイル出力(debug)
file_handler = logging.handlers.TimedRotatingFileHandler('collect/executions/execution', when='h', interval=1)
file_handler.suffix = "%Y%m%d%H%M%S"
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(file_handler)

import os
import csv
import json
import configparser
import websocket
from datetime import datetime
from pytz import timezone
from dateutil import parser

from common.utils import dt2str


inifile = configparser.ConfigParser()
inifile.read('config.ini', 'UTF-8')
api_key = inifile.get('config', 'api_key')
api_secret = inifile.get('config', 'api_secret')

def on_open(ws):
    # 1分足のデータを要求するために送信するデータ
    channels = {
        'op': 'subscribe',
        'args': [
            'trade:XBTUSD'
        ]
    }
    ws.send(json.dumps(channels))

def on_message(ws, message):
    message = json.loads(message)

    if 'data' not in message:
        return

    for d in message['data']:
        now = int(round(datetime.now().timestamp() * 1000))
        exec_data = [now, d['timestamp'], d['side'], d['price'], d['size']]
        timestamp_jst = dt2str(parser.parse(d['timestamp']).astimezone(timezone('Asia/Tokyo')))
        exec_data.append(dt2str(datetime.now()))
        exec_data.append(timestamp_jst)
        logger.info('{},{},{},{},{},{},{}'.format(exec_data[0], exec_data[1], exec_data[2], exec_data[3], exec_data[4], exec_data[5], exec_data[6]))

def on_close(ws):
    print('close')
    # サーバとの切断時に実行する処理

def on_error(ws, error):
    print('error')
    # エラー発生時に実行する処理

if __name__=='__main__':
    # サーバとのデータのやりとりを表示するため、Trueを指定する。（確認したくないのであればFalseで問題ないです）
    websocket.enableTrace(True)

    # 接続先URLと各コールバック関数を引数に指定して、WebSocketAppのインスタンスを作成
    ws = websocket.WebSocketApp(url='wss://www.bitmex.com/realtime',
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close,
                                on_error=on_error)

    # BitMEXのサーバへ接続する
    ws.run_forever()