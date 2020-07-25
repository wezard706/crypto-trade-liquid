import logging

logger = logging.getLogger('crypto')

import sys

from common.utils import str2timestamp


class ResourceManager:
    def __init__(self, ticker, ohlcv_list, bids, asks, inago, tm):
        self.ticker = ticker
        self.ohlcv_list = ohlcv_list
        self.bids = bids
        self.asks = asks
        self.inago = inago
        self.tm = tm

    def fetch_ticker(self):
        now = self.tm.now * 1000
        target = self.ticker[self.ticker.timestamp == now]
        if len(target) == 0:
            logger.debug('invalid target size in fetch_ticker')
            sys.exit()
        return dict(target.iloc[0])

    def fetch_ohlcv(self, candle_type, since, limit=None):
        ohlcv = self.ohlcv_list[candle_type]
        target = ohlcv[ohlcv.timestamp > since].ix[:, ['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        if len(target) == 0:
            logger.debug('invalid target size in fetch_ohlcv')
            sys.exit()

        if limit is not None:
            if len(target) > limit:
                target = target.iloc[:limit]
        return target.values.tolist()

    def fetch_order_book(self):
        # limitは実装が面倒なので引数に取らない

        def format_order_book(bids, asks):
            formatted_bids = []
            formatted_asks = []
            for i in range(25):
                col_idx = '{0:02d}'.format(i)
                bid_price = bids.loc['price' + col_idx]
                bid_amount = bids.loc['amount' + col_idx]
                ask_price = asks.loc['price' + col_idx]
                ask_amount = asks.loc['amount' + col_idx]
                formatted_bids.append([bid_price, bid_amount])
                formatted_asks.append([ask_price, ask_amount])
            return {'bids': formatted_bids, 'asks': formatted_asks, 'timestamp': bids.timestamp}

        now = self.tm.now * 1000
        bids = self.bids[self.bids.timestamp == now]
        asks = self.asks[self.asks.timestamp == now]
        if (len(bids) == 0) or (len(asks) == 0):
            logger.debug('invalid target size in fetch_order_book')
            sys.exit()
        return format_order_book(bids.iloc[0], asks.iloc[0])

    def fetch_inago(self, start_time, end_time):
        start_time = str2timestamp(start_time) * 1000
        end_time = str2timestamp(end_time) * 1000
        return self.inago[(start_time <= self.inago.timestamp) & (self.inago.timestamp <= end_time)]