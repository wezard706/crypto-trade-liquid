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
import json
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from common.utils import dt2str, dict2str

import mysql.connector
import configparser
inifile = configparser.ConfigParser()
inifile.read('config.ini', 'UTF-8')
user = inifile.get('mysql', 'user')
password = inifile.get('mysql', 'password')
url = urlparse('mysql://' + user + ':' + password + '@localhost:3306/crypto')
conn = mysql.connector.connect(
    host=url.hostname or 'localhost',
    port=url.port or 3306,
    user=url.username or 'root',
    password=url.password or '',
    database=url.path[1:],
)

class InagoHandler(BaseHTTPRequestHandler):
    def save_response(self, res):
        # クエリを作成
        query = 'INSERT INTO crypto.inago (' \
                'board_name, taker_side, volume, last_price, pair_currency, from_unix_time, to_unix_time, from_datetime, to_datetime, timestamp)' \
                'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        from_datetime = dt2str(datetime.fromtimestamp(int(res['fromUnixTime']) / 1000))
        to_datetime = dt2str(datetime.fromtimestamp(int(res['toUnixTime']) / 1000))
        now = int(round(datetime.now().timestamp() * 1000))
        insert_data = list(res.values()) + [from_datetime, to_datetime, now]
        cur = conn.cursor()
        try:
            # クエリを実行
            cur.execute(query, insert_data)
            conn.commit()
        except:
            conn.rollback()
            logger.error(traceback.format_exc())
            raise Exception
        cur.close()

    def do_POST(self):
        content_len = int(self.headers.get('content-length'))
        res = json.loads(self.rfile.read(content_len).decode('utf-8'))

        if res['boardName'] == 'BitMEX_XBTUSD':
            logger.debug('save response: {}'.format(dict2str(res)))
            self.save_response(res)

if __name__=='__main__':
    server_address = ('localhost', 8080)
    httpd = HTTPServer(server_address, InagoHandler)
    httpd.serve_forever()