import os
import sys
import pickle
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta
#%matplotlib inline
import matplotlib.pyplot as plt
from IPython.display import Image, display_png
import os
import fnmatch
from common.utils import str2dt, format_dt


def generate_ticker(executions):
    '''
    約定履歴から1秒ごとのtickerを作成
    約定履歴: timestamp, side, price, amount
    ticker: timestamp, bid, bid_volume, ask, ask_volume, open, high, low, close, volume
    '''
    executions.index = pd.to_datetime(executions.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))

    executions = executions.resample('S').last()

    # bid, askを算出
    bids = executions.loc[executions.side == 'Sell'].resample('S').ffill()
    bids = bids.drop(['timestamp', 'side'], axis=1)
    asks = executions.loc[executions.side == 'Buy'].resample('S').ffill()
    asks = asks.drop(['timestamp', 'side'], axis=1)
    bids_asks = pd.merge(bids, asks, how='inner', left_index=True, right_index=True)
    bids_asks.columns = ['bid', 'bid_volume', 'ask', 'ask_volume']
    # OHLCVを算出
    ohlcv = bids.price.resample('S').ohlc()
    ohlcv['volume'] = bids.amount.resample('S').sum()
    ohlcv['timestamp'] = pd.Series(ohlcv.index).apply(lambda x: datetime.timestamp(x) * 1000).values
    # ticker作成
    ticker = pd.merge(bids_asks, ohlcv, how='inner', left_index=True, right_index=True)
    ticker['timestamp'] = pd.Series(ticker.index).apply(lambda x: datetime.timestamp(x) * 1000).values
    return ticker.ix[:,['timestamp', 'bid', 'bid_volume', 'ask', 'ask_volume', 'open', 'high', 'low', 'close', 'volume']]


def generate_ohlcv(executions, candle_type):
    '''
    約定履歴から１分、５分、１時間のOHLCVを作成
    約定履歴: timestamp, side, price, amount
    OHLCV: timestamp, bid, bid_volume, ask, ask_volume, open, high, low, close, volume
    '''
    executions.index = pd.to_datetime(executions.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))

    # 1秒ごとにグルーピング
    executions = executions.resample('S').last()

    # bid, askを算出
    bids = executions.loc[executions.side == 'Sell'].resample('S').ffill()

    # OHLCVを作成
    if candle_type == '1s':
        freq = 'S'
    if candle_type == '1m':
        freq = 'T'
    elif candle_type == '5m':
        freq = '5T'
    elif candle_type == '1h':
        freq = 'H'

    ohlcv = bids.price.resample(freq).ohlc()
    ohlcv['volume'] = bids.amount.resample(freq).sum()
    ohlcv['timestamp'] = pd.Series(ohlcv.index).apply(lambda x: datetime.timestamp(x) * 1000).values
    return ohlcv

def generate_active_ohlcv(df, candle_type):
    '''
    1秒足から未確定足を含むOHLCVを作成
    df: DataFrame (index: datetimeindex, column: open, high, low, close, volume timestamp)
    candle_type: '1m' or '5m' or '1h'
    '''

    ohlcv = df.values
    open = ohlcv[0, 0]
    high = -1
    low = 999999
    close = ohlcv[0, 3]
    volume = ohlcv[0, 4]

    if candle_type == '1m':
        div = 60 * 1000
    elif candle_type == '5m':
        div = 60 * 5 * 1000
    elif candle_type == '1h':
        div = 60 * 60 * 1000

    active_ohlcv = []
    for i in range(ohlcv.shape[0]):
        row = ohlcv[i]

        # １分ごとにリセット
        timestamp = row[5]
        if timestamp % div == 0:
            open = row[0]
            high = -1
            low = 999999
            volume = 0

        close = row[3]
        volume += row[4]
        if row[1] > high:
            high = row[1]
        if row[2] < low:
            low = row[2]
        active_ohlcv.append([open, high, low, close, volume, timestamp])

    active_ohlcv = pd.DataFrame(active_ohlcv)
    active_ohlcv.columns = ['open', 'high', 'low', 'close', 'volume', 'timestamp']
    active_ohlcv.index = pd.to_datetime(active_ohlcv.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))
    return active_ohlcv

def read_csvs(dirpath, start_dt, end_dt, pattern=None, prefix=None):
    files = sorted(os.listdir(dirpath))

    if pattern is not None:
        files = [file for file in files if fnmatch.fnmatch(file, pattern)]

    ret = None
    for file in files:
        if prefix is None:
            dt = datetime.strptime(file, '%Y%m%d%H%M%S')
        else:
            dt = datetime.strptime(file.replace(prefix, ''), '%Y%m%d%H%M%S')

        if (start_dt <= dt) and (dt <= end_dt):
            print('read file: {}'.format(file))
            df = pd.read_csv(os.path.join(dirpath, file), header=None)

            if ret is None:
                ret = df
            else:
                ret = pd.concat([ret, df])
    return ret

if __name__=='__main__':
    start_dt = str2dt('2019-04-06 19:00:00')
    end_dt = str2dt('2019-04-06 21:00:00')

    # ticker, ohlcv
    executions = read_csvs('collect/executions', start_dt, end_dt, pattern='execution.*', prefix='execution.')
    executions.columns = ['timestamp', 'datetime_utc_bitmex', 'side', 'price', 'amount', 'datetime',
                          'datetime_jst_bitmex']
    executions = executions.drop(['datetime', 'datetime_utc_bitmex', 'datetime_jst_bitmex'], axis=1)
    executions['side'] = executions['side'].str.replace(' ', '') # 後で消す
    ticker = generate_ticker(executions)
    ohlcv = generate_ohlcv(executions, candle_type='1s')
    active_ohlcv_1m = generate_active_ohlcv(ohlcv, candle_type='1m')
    active_ohlcv_5m = generate_active_ohlcv(ohlcv, candle_type='5m')
    active_ohlcv_1h = generate_active_ohlcv(ohlcv, candle_type='1h')

    # orderbook
    header = ['timestamp']
    for i in range(25):
        for col in ['amount', 'price']:
            header.append(col + '{0:02d}'.format(i))
    bids = read_csvs('collect/orderbook/bids', start_dt, end_dt, pattern='bid.*', prefix='bid.')
    bids.columns = header
    bids.index = pd.to_datetime(bids.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))
    bids = bids.resample('S').last()
    bids = bids.resample('S').ffill()
    asks = read_csvs('collect/orderbook/asks', start_dt, end_dt, pattern='ask.*', prefix='ask.')
    asks.columns = header
    asks.index = pd.to_datetime(asks.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))
    asks = asks.resample('S').last()
    asks = asks.resample('S').ffill()

    '''
    print('{} - {}'.format(ticker.index[0], ticker.index[-1]))
    print('{} - {}'.format(ohlcv['1m'].index[0], ohlcv['1m'].index[-1]))
    print('{} - {}'.format(bids.index[0], bids.index[-1]))
    print('{} - {}'.format(asks.index[0], asks.index[-1]))

    max_start = max([ticker.index[0], ohlcv['1m'].index[0], bids.index[0], asks.index[0]])
    min_end = min([ticker.index[-1], ohlcv['1m'].index[-1], bids.index[-1], asks.index[-1]])

    ticker = ticker.loc[(max_start <= ticker.index) & (ticker.index < min_end)]
    ohlcv['1m'] = ohlcv['1m'].loc[(max_start <= ohlcv['1m'].index) & (ohlcv['1m'].index < min_end)]
    bids = bids.loc[(max_start <= bids.index) & (bids.index < min_end)]
    asks = asks.loc[(max_start <= asks.index) & (asks.index < min_end)]

    print('{} - {}'.format(ticker.index[0], ticker.index[-1]))
    print('{} - {}'.format(ohlcv['1m'].index[0], ohlcv['1m'].index[-1]))
    print('{} - {}'.format(bids.index[0], bids.index[-1]))
    print('{} - {}'.format(asks.index[0], asks.index[-1]))

    # inago
    inago = pd.read_csv('collect/inago/inago.csv', sep='\t',
                        names=['id', 'board_name', 'taker_side', 'volume', 'last_price', 'pair_currency',
                               'from_unix_time', 'to_unix_time', 'from_datetime', 'to_datetime', 'timestamp'])
    inago.index = pd.to_datetime(inago.timestamp.apply(lambda x: datetime.fromtimestamp(x / 1000)))
    inago = inago.resample('S').last()
    inago['timestamp'] = pd.Series(inago.index).apply(lambda x: datetime.timestamp(x) * 1000).values
    inago = inago.loc[(max_start <= inago.index) & (inago.index < min_end)]
    '''