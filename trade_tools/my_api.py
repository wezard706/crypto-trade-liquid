import pandas as pd
import mysql.connector
from datetime import datetime
from abc import ABCMeta, abstractmethod

from trade_tools.trade_utils import init_exchange, format_orderbook

class APIBase(metaclass=ABCMeta):
    def __init__(self, exchange_name):
        self.exchange = init_exchange(exchange_name)

    @abstractmethod
    def fetch_ticker(self, pair):
        pass

    @abstractmethod
    def fetch_ohlcv(self, pair, candle_type, since=None, limit=None):
        pass

    @abstractmethod
    def fetch_order_book(self, pair, limit=None):
        pass

    @abstractmethod
    def create_order(self, pair, type, side, amount, price):
        pass

    @abstractmethod
    def cancel_order(self, order_id):
        pass

    @abstractmethod
    def fetch_order(self, order_id, pair):
        pass

    @abstractmethod
    def fetch_inago(self, account, start_time, end_time):
        pass

    def private_get_position(self):
        return self.exchange.private_get_position()

    def fetch_current_price(self, pair, candle_type, since, limit=None):
        unixtime, open, high, low, close, volume = self.fetch_ohlcv(pair, candle_type, since, limit)[-1]
        side = 'sell' if open - close >= 0 else 'buy'
        curr_price = close
        return curr_price, side

class API(APIBase):
    def __init__(self, exchange_name):
        super(API, self).__init__(exchange_name)

    def fetch_ticker(self, pair):
        return self.exchange.fetch_ticker(pair)

    def fetch_ohlcv(self, pair, candle_type, since=None, limit=None):
        return self.exchange.fetch_ohlcv(pair, candle_type, since, limit)

    def fetch_order_book(self, pair, limit=None):
        return self.exchange.fetch_order_book(pair, limit)

    def create_order(self, pair, type, side, amount, price):
        return self.exchange.create_order(pair, type, side, amount, price)

    def cancel_order(self, order_id):
        return self.exchange.cancel_order(order_id)

    def fetch_order(self, order_id, pair):
        return self.exchange.fetch_order(order_id, pair)

    def fetch_inago(self, account, start_time, end_time):
        conn = mysql.connector.connect(**account)
        cur = conn.cursor(dictionary=True)
        cur.execute('SELECT * FROM crypto.inago WHERE %s <= to_datetime AND to_datetime <= %s', [start_time, end_time])
        df_inago = pd.DataFrame(cur.fetchall())
        cur.close()
        conn.close()
        return df_inago

class APISim(APIBase):
    def __init__(self, exchange_name, rm):
        super(APISim, self).__init__(exchange_name)
        self.orders = {}
        self.counter = 0
        self.profit = 0
        self.rm = rm

    def fetch_ticker(self, pair):
        return self.rm.fetch_ticker()

    def fetch_ohlcv(self, pair, candle_type, since=None, limit=None):
        return self.rm.fetch_ohlcv(candle_type, since, limit)

    def fetch_order_book(self, pair, limit=None):
        return self.rm.fetch_order_book()

    def create_order(self, pair, type, side, amount, price):
        order = {
            'info': {'orderID': self.counter, 'symbol': pair, 'side': side, 'orderQty': amount, 'price': price, 'ordType': type},
            'id': self.counter,
            'symbol': pair,
            'type': type,
            'side': side,
            'price': price,
            'amount': amount,
            'status': 'open',
        }
        self.orders[self.counter] = order
        self.counter += 1
        return order

    def cancel_order(self, order_id):
        order = self.orders[order_id]
        order['status'] = 'canceled'
        return order

    def fetch_order(self, order_id, pair):
        order = self.orders[order_id]

        # 板が注文価格になったら status を closed に設定
        orderbook = format_orderbook(self.fetch_order_book(pair))
        if order['side'] == 'buy':
            curr_price = float(orderbook.loc[orderbook['side'] == 'asks', 'price'].iloc[0])
            if curr_price <= order['price']:
                order['status'] = 'closed'
        else:
            curr_price = float(orderbook.loc[orderbook['side'] == 'bids', 'price'].iloc[0])
            if curr_price >= order['price']:
                order['status'] = 'closed'

        # 利益を計算する
        # if True:
        if order['status'] == 'closed':
            df_orders = pd.DataFrame(list(self.orders.values())).sort_values('id')
            df_orders = df_orders[df_orders['status'] == 'closed']

            if len(df_orders) % 2 == 1:
                df_orders = df_orders[:-1]

            prev_price = -1
            for i, row in df_orders.iterrows():
                if i % 2 == 1:
                    if row['side'] == 'buy':
                        self.profit += prev_price - row['price']
                    else:
                        self.profit += row['price'] - prev_price
                else:
                    prev_price = row['price']
        return order

    def fetch_inago(self, account, start_time, end_time):
        return self.rm.fetch_inago(start_time, end_time)
