import logging

# logger
logger = logging.getLogger('crypto')
logger.setLevel(logging.DEBUG)
format = logging.Formatter('[%(levelname)s] %(asctime)s, %(message)s')
# 標準出力
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(format)
logger.addHandler(stream_handler)

import traceback
import os
import sys
import time
import configparser
from urllib.parse import urlparse
import pandas as pd
from datetime import datetime, timedelta

from common.utils import dt2str, str2timestamp, merge_dicts
from trade_tools.trade_utils import calc_horizon, format_orderbook, get_orderbook_around_horizon, get_horizon_closest_to_price
from trade_tools.my_api import APISim, API
from trade_tools.rm import ResourceManager
from trade_tools.tm import TimeManager

inifile = configparser.ConfigParser()
inifile.read('config.ini', 'UTF-8')
user = inifile.get('mysql', 'user')
password = inifile.get('mysql', 'password')
url = urlparse('mysql://' + user + ':' + password + '@localhost:3306/crypto')
account = {
    'host': url.hostname,
    'port': url.port,
    'user': url.username,
    'password': url.password,
    'database': url.path[1:]
}

def calc_order_price(horizon, orderbook, scope=3, threshold=10000, shift=0.5):
    # ライン付近の板を取得
    near_orderbook = get_orderbook_around_horizon(orderbook, horizon, scope)

    # 求めた範囲内の注文数を計算
    total_order_num = near_orderbook.sum()['order_num']

    # 注文数が閾値を超えたら注文する
    order_price = -1
    if total_order_num > threshold:
        # 求めた範囲で最大の板の少し内側を注文価格とする
        orderbook_max = orderbook[orderbook.index == near_orderbook['order_num'].idxmax()]
        if orderbook_max['side'].values == 'bids':
            order_price = (orderbook_max['price'] + shift).iloc[0]
        else:
            order_price = (orderbook_max['price'] - shift).iloc[0]
    return order_price

def update_horizon(api, pair, candle_types, sinces):
    horizons = {}
    for candle_type, since in zip(candle_types, sinces):
        ohlcv = api.fetch_ohlcv(pair, candle_type, since)
        df_ohlcv = pd.DataFrame(ohlcv, columns=['unixtime', 'open', 'high', 'low', 'close', 'volume'])
        # 未確定足は含めない
        df_ohlcv = df_ohlcv.iloc[:-1]
        df_ohlcv['unixtime'] = df_ohlcv['unixtime'] / 1000
        df_ohlcv.index = df_ohlcv.unixtime.map(lambda x: datetime.fromtimestamp(x))
        tmp = calc_horizon(df_ohlcv, min_whisker_len=0, threshold_whisker_diff=0)
        horizons = merge_dicts(tmp, horizons)
    return horizons

def is_contract_doten(order, pair, candle_type, api, tm, max_wait_time, target_horizon, is_backtest):
    start = tm.now if is_backtest else time.time()
    elapsed_time = 0
    prev_side = 'buy' if order['side'] == 'sell' else 'sell'
    while order['status'] != 'closed':
        if elapsed_time > max_wait_time:
            logger.debug('max wait time is over')
            return False

        since = (tm.now - 60) * 1000 if is_backtest else (datetime.now().timestamp() - 60) * 1000
        curr_price, _ = api.fetch_current_price(pair, candle_type, since)
        if (prev_side == 'buy' and target_horizon < curr_price) or (prev_side == 'sell' and target_horizon > curr_price):
            logger.debug('horizon is penetrated')
            return False

        if is_backtest:
            tm.forward_timestamp(5)
        else:
            time.sleep(5)

        # 更新
        order = api.fetch_order(order['id'], pair)
        elapsed_time = tm.now - start if is_backtest else time.time() - start
    return True

def is_contract_inago(order, pair, api, tm, max_wait_time, is_backtest):
    start = tm.now if is_backtest else time.time()
    elapsed_time = 0
    while order['status'] != 'closed':
        if elapsed_time > max_wait_time:
            logger.debug('max wait time is over')
            return False

        if is_backtest:
            tm.forward_timestamp(5)
        else:
            time.sleep(5)

        # 更新
        order = api.fetch_order(order['id'], pair)
        elapsed_time = tm.now - start if is_backtest else time.time() - start
    return True

def order_limits_by_horizon(horizons, api, pair):
    horizon_and_order = {}
    curr_price = api.fetch_ticker(pair)['close']
    orderbook = format_orderbook(api.fetch_order_book(pair, limit=1000))
    for horizon in horizons:
        order_price = calc_order_price(horizon, orderbook)
        if order_price != -1:
            logger.debug('limit order at {}'.format(order_price))
            if order_price > curr_price:
                order = api.create_order(pair, type='limit', side='sell', amount=1, price=order_price)
            else:
                order = api.create_order(pair, type='limit', side='buy', amount=1, price=order_price)
            if order['id'] == 85:
                print('side: {}'.format(order['side']))
                print('current price: {}'.format(curr_price))
                print('order price: {}'.format(order_price))
            horizon_and_order[horizon] = order
    return horizon_and_order

def main():
    exchange_name = 'bitmex'
    pair = 'BTC/USD'
    candle_type = '1m'
    max_wait_time = 300
    is_test = True

    if is_test:
        ticker = pd.read_csv('collect/format_data/ticker.csv')
        ohlcv_1m = pd.read_csv('collect/format_data/ohlcv_1m.csv')
        ohlcv_list = {'1m': ohlcv_1m}
        bids = pd.read_csv('collect/format_data/bids.csv')
        asks = pd.read_csv('collect/format_data/asks.csv')
        inago = pd.read_csv('collect/format_data/inago.csv')
        tm = TimeManager(str2timestamp('2019-03-25 01:00:00'))
        rm = ResourceManager(ticker, ohlcv_list, bids, asks, inago, tm)
        api = APISim(exchange_name, rm)
    else:
        api = API(exchange_name)

    # ラインを更新
    candle_types = ['1m']
    if is_test:
        sinces = [(tm.now - 3600) * 1000]
    else:
        sinces = [(datetime.now() - timedelta(hours=1)).timestamp() * 1000]
    horizons = update_horizon(api, pair, candle_types, sinces)

    # 指値を入れる
    horizon_and_order = order_limits_by_horizon(horizons, api, pair)

    profits = 0
    start = tm.now if is_test else time.time()
    elapsed_time = 0
    prev_time = dt2str(datetime.fromtimestamp(tm.now - 3)) if is_test else dt2str(datetime.now() - timedelta(seconds=3))
    while True:
        logger.debug('current time: {}'.format(datetime.fromtimestamp(tm.now)))
        logger.debug('current price: {}'.format(api.fetch_ticker(pair)['close']))

        # イナゴ発動（毎回コネクションを貼り直さないとinago_serverで格納したデータが反映されない）
        curr_time = dt2str(datetime.fromtimestamp(tm.now - 3)) if is_test else dt2str(datetime.now() - timedelta(seconds=3))
        inago = api.fetch_inago(account, prev_time, curr_time)
        if len(inago) > 0:
            logger.debug('InagoFlyer is screaming ...')

            # 一番近くのラインを取得
            inago_side = inago.iloc[-1].loc['taker_side']
            curr_price = api.fetch_ticker(pair)['close']
            if inago_side == 'buy':
                target_horizon = get_horizon_closest_to_price(horizons, curr_price, scope=1000, direction='upper')
            else:
                target_horizon = get_horizon_closest_to_price(horizons, curr_price, scope=1000, direction='lower')

            # そのラインと現在価格の距離が10$未満の場合
            if abs(target_horizon - curr_price) < 10:
                # 指値が約定した場合
                horder = horizon_and_order[target_horizon]
                if is_contract_inago(horder, pair, api, tm, max_wait_time, is_test):
                    print('inago side: {}, horder: {}'.format(inago_side, horder['side']))
                    logger.debug('inago order {} is contracted: {}'.format(horder['id'], horder))

                    # ドテンで指値を入れる
                    since = (tm.now - 60) * 1000 if is_test else (datetime.now().timestamp() - 60) * 1000
                    curr_price, _ = api.fetch_current_price(pair, candle_type, since)
                    if inago_side == 'buy':
                        dorder = api.create_order(pair, type='limit', side='buy', amount=1, price=curr_price - 2)
                    else:
                        dorder = api.create_order(pair, type='limit', side='sell', amount=1, price=curr_price + 2)

                    # ドテンが約定しない場合
                    if not is_contract_doten(dorder, pair, candle_type, api, tm, max_wait_time, target_horizon, is_test):
                        if inago_side == 'buy':
                            order = api.create_order(pair, type='market', side='buy', amount=1, price=curr_price)
                            profit = (horder['price'] - order['price']) - order['price'] * 0.075
                        else:
                            order = api.create_order(pair, type='market', side='sell', amount=1, price=curr_price)
                            profit = (order['price'] - horder['price']) - order['price'] * 0.075
                        api.cancel_order(dorder['id'])
                        logger.debug('doten order {} is canceled'.format(dorder['id'], dorder))
                    else:
                        logger.debug('doten order {} is contracted: {}'.format(dorder['id'], dorder))
                        if horder['side'] == 'buy':
                            profit = (horder['price'] - dorder['price']) + dorder['price'] * 0.025
                        else:
                            profit = (dorder['price'] - horder['price']) + dorder['price'] * 0.025

                    # 利益計算
                    profits += profit
                    logger.debug('current profit: {}'.format(profits))

        if is_test:
            tm.forward_timestamp(1)
        else:
            time.sleep(1)

        # 更新
        if elapsed_time > 300:
            # 水平線を引く
            if is_test:
                sinces = [(tm.now - 3600) * 1000]
            else:
                sinces = [(datetime.now() - timedelta(hours=1)).timestamp() * 1000]
            horizons = update_horizon(api, pair, candle_types, sinces)

            # 指値を入れる
            for horizon, order in horizon_and_order.items():
                api.cancel_order(order['id'])
            horizon_and_order = order_limits_by_horizon(horizons, api, pair)
            start = tm.now
        elapsed_time = tm.now - start if is_test else time.time() - start
        prev_time = curr_time

if __name__=='__main__':
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        logger.debug('process reboot')
        os.execv(sys.executable, [sys.executable] + ['trade/main.py'])
