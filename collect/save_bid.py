import logging.handlers

# logger
logger = logging.getLogger('crypto-collect')
logger.setLevel(logging.DEBUG)
format = logging.Formatter('[%(levelname)s] %(asctime)s, %(message)s')
# 標準出力
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(format)
logger.addHandler(stream_handler)
# ファイル出力(debug)
file_handler = logging.handlers.TimedRotatingFileHandler('collect/orderbook/bids/bid', when='h', interval=1)
file_handler.suffix = "%Y%m%d%H%M%S"
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(file_handler)

import json
import websocket
from datetime import datetime
from collections import OrderedDict

id_and_price = {}
bids = OrderedDict()
asks = OrderedDict()

def on_open(ws):
    # 1分足のデータを要求するために送信するデータ
    channels = {
        'op': 'subscribe',
        'args': [
            'orderBookL2_25:XBTUSD'
        ]
    }
    ws.send(json.dumps(channels))

def update_orderbook(message):
    global id_and_price
    global bids
    global asks

    if message['action'] == 'partial':
        for d in message['data']:
            id_and_price[d['id']] = d['price']
            if d['side'] == 'Buy':
                bids[d['id']] = [d['price'], d['size']]
            else:
                asks[d['id']] = [d['price'], d['size']]
    elif message['action'] == 'update':
        for d in message['data']:
            if d['side'] == 'Buy':
                bids[d['id']] = [id_and_price[d['id']], d['size']]
            else:
                asks[d['id']] = [id_and_price[d['id']], d['size']]
    elif message['action'] == 'insert':
        for d in message['data']:
            id_and_price[d['id']] = d['price']
            if d['side'] == 'Buy':
                bids[d['id']] = [d['price'], d['size']]
            else:
                asks[d['id']] = [d['price'], d['size']]
    else:
        for d in message['data']:
            if d['side'] == 'Buy':
                bids.pop(d['id'])
            else:
                asks.pop(d['id'])
        bids = OrderedDict(sorted(bids.items(), key=lambda x: x[0]))
    asks = OrderedDict(sorted(asks.items(), key=lambda x: x[0], reverse=True))
    return bids, asks

def on_message(ws, message):
    message = json.loads(message)

    if 'data' not in message:
        return

    if 'table' in message:
        now = str(round(datetime.now().timestamp() * 1000))
        bids, asks = update_orderbook(message)

        formatted_bids = now + ','
        for id, bid in bids.items():
            formatted_bids += str(bid[0]) + ','
            formatted_bids += str(bid[1]) + ','
        logger.info(formatted_bids[:-1])

def on_close(ws):
    print('close')

def on_error(ws, error):
    import sys
    sys.exit()

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