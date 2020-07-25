import ccxt
import numpy as np
from collections import ChainMap
import copy
import configparser
import pandas as pd

inifile = configparser.ConfigParser()
inifile.read('config.ini', 'UTF-8')
api_key = inifile.get('config', 'api_key')
api_secret = inifile.get('config', 'api_secret')

def init_exchange(exchange):
    params = {
        'apiKey': api_key,
        'secret': api_secret
    }
    if exchange == 'bitmex':
        return ccxt.bitmex(params)
    elif exchange == 'bitfinex':
        return ccxt.bitfinex(params)
    else:
        return

def format_orderbook(orderbook):
    bids = pd.DataFrame(orderbook['bids'], columns=['price', 'order_num'])
    asks = pd.DataFrame(orderbook['asks'], columns=['price', 'order_num'])
    orderbook = pd.concat([bids, asks]).reset_index(drop=True)
    orderbook['side'] = ['bids'] * len(bids) + ['asks'] * len(asks)
    return orderbook

def calc_horizon(df, min_whisker_len, threshold_whisker_diff):
    '''
    1. ひげの長さが min_whisker_len以上のローソク足を選択
    2. その中で高値が近い（高値間の差がthreshold_whisker_diff以下）２本のローソクの高値の平均値を求める

    min_whisker_len: ひげの長さの最小値
    threshold_whisker_diff:
    horizons: key -> 水平線の値、value -> 水平線を引く根拠となるローソク足の時間のリスト（水平線の値が同じであれば同じkeyとして登録されるためリストの長さは２とは限らない）
    '''
    # ひげが長いローソク足を選択
    whisker_len = np.where(df['open'] >= df['close'], df['high'] - df['open'], df['high'] - df['close'])
    df_long_whisker = df.loc[(whisker_len >= min_whisker_len)]

    # 高値が近いローソク足の高値の平均値を求める
    horizons = {}
    for dt1, ohlcv1 in df_long_whisker.iterrows():
        for dt2, ohlcv2 in df_long_whisker.iterrows():
            if dt1 == dt2:
                continue

            if (float(np.abs(ohlcv1['high'] - ohlcv2['high'])) <= threshold_whisker_diff):  # 高値と高値
                horizon = np.mean([ohlcv1['high'], ohlcv2['high']])
                if horizon in horizons.keys():
                    horizons[horizon].append(dt1)
                    horizons[horizon].append(dt2)
                else:
                    horizons[horizon] = [dt1, dt2]
            if (float(np.abs(ohlcv1['high'] - ohlcv2['low'])) <= threshold_whisker_diff):  # 高値と低値
                horizon = np.mean([ohlcv1['high'], ohlcv2['low']])
                if horizon in horizons.keys():
                    horizons[horizon].append(dt1)
                    horizons[horizon].append(dt2)
                else:
                    horizons[horizon] = [dt1, dt2]
            if (float(np.abs(ohlcv1['low'] - ohlcv2['low'])) <= threshold_whisker_diff):  # 低値と低値
                horizon = np.mean([ohlcv1['low'], ohlcv2['low']])
                if horizon in horizons.keys():
                    horizons[horizon].append(dt1)
                    horizons[horizon].append(dt2)
                else:
                    horizons[horizon] = [dt1, dt2]
            if (float(np.abs(ohlcv1['low'] - ohlcv2['high'])) <= threshold_whisker_diff):  # 低値と高値
                horizon = np.mean([ohlcv1['low'], ohlcv2['high']])
                if horizon in horizons.keys():
                    horizons[horizon].append(dt1)
                    horizons[horizon].append(dt2)
                else:
                    horizons[horizon] = [dt1, dt2]

    # 重複を削除
    horizons_c = copy.copy(horizons)
    for h, dts in horizons_c.items():
        horizons[h] = list(set(dts))
    # 支点の右側で他のローソク足と重畳しないか確認
    unuses = []
    curr_price = df.ix[-1, 'close']
    for horizon, dts in horizons.items():
        target = df[df.index > min(dts)]
        crossed_ohlcv = target.loc[(target['high'] > horizon) & (target['low'] < horizon)]
        if len(crossed_ohlcv) > 0:
            unuses.append(horizon)
        # 現在価格に近いラインは削除
        elif np.abs(horizon - curr_price) <= 1:
            unuses.append(horizon)

    for unuse in unuses:
        horizons.pop(unuse)
    return horizons

def get_orderbook_around_horizon(orderbook, horizon, scope, direction='both'):
    '''
    ラインの近くの板を取得する
    '''

    idx_orderbook = (orderbook['price'] - horizon).abs().idxmin()
    if direction == 'both':
        idx_start = idx_orderbook - scope if idx_orderbook - scope >= 0 else 0
        idx_end = idx_orderbook + scope if idx_orderbook + scope <= len(orderbook) else len(orderbook) - 1
    elif direction == 'upper':
        idx_start = idx_orderbook
        idx_end = idx_orderbook + scope if idx_orderbook + scope <= len(orderbook) else len(orderbook) - 1
    else:
        idx_start = idx_orderbook - scope if idx_orderbook - scope >= 0 else 0
        idx_end = idx_orderbook
    near_orderbook = orderbook.iloc[idx_start:idx_end + 1]
    return near_orderbook

def get_horizon_closest_to_price(horizons, curr_price, scope=9999999, direction='both'):
    '''
    現在価格に最も近いラインを取得する
    '''

    # scope内のラインを取得
    upper_price = curr_price + scope
    lower_price = curr_price - scope
    if direction == 'both':
        near_horizons = [horizon for horizon in horizons if (lower_price < horizon) and (horizon < upper_price)]
    elif direction == 'upper':
        near_horizons = [horizon for horizon in horizons if (curr_price < horizon) and (horizon < upper_price)]
    else:
        near_horizons = [horizon for horizon in horizons if (lower_price < horizon) and (horizon < curr_price)]

    if len(near_horizons) == 0:
        return -1

    # 取得したラインの中で最も現在価格に近いラインを返す
    min_dist = 9999999
    nearest_horizon = -1
    for horizon in near_horizons:
        if abs(horizon - curr_price) < min_dist:
            min_dist = abs(horizon - curr_price)
            nearest_horizon = horizon
    return nearest_horizon

def exist_limit_orders_around_horizon(pair, horizon, exchange_name, scope):
    '''
    ライン付近に他の取引所で板があるか
    '''

    exchange = init_exchange(exchange_name)

    # 板を取得
    orderbook = format_orderbook(exchange.fetch_order_book(pair, limit=1000))
    near_orderbook = get_orderbook_around_horizon(orderbook, horizon, scope)
    # 求めた範囲内の注文数を計算
    total_order_num = near_orderbook.sum()['order_num']

    # 注文数が閾値を下回ったら指値を外す
    threshold = 1
    if total_order_num < threshold:
        return False
    else:
        return True
