# coding=utf-8

import json
import threading

from autobahn.twisted.websocket import WebSocketClientFactory, \
    WebSocketClientProtocol, \
    connectWS
from twisted.internet import reactor, ssl
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.error import ReactorAlreadyRunning

from binance.client import Client


class BinanceFuturesClientProtocol(WebSocketClientProtocol):

    def __init__(self):
        super(WebSocketClientProtocol, self).__init__()

    def onConnect(self, response):
        # reset the delay after reconnecting
        self.factory.resetDelay()

    def onMessage(self, payload, isBinary):
        if not isBinary:
            try:
                payload_obj = json.loads(payload.decode('utf8'))
            except ValueError:
                pass
            else:
                self.factory.callback(payload_obj)


class BinanceFuturesReconnectingClientFactory(ReconnectingClientFactory):
    # set initial delay to a short time
    initialDelay = 0.1

    maxDelay = 10

    maxRetries = 5


class BinanceFuturesClientFactory(WebSocketClientFactory, BinanceFuturesReconnectingClientFactory):
    protocol = BinanceFuturesClientProtocol
    _reconnect_error_payload = {
        'e': 'error',
        'm': 'Max reconnect retries reached'
    }

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)

    def clientConnectionLost(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)


class BinanceSocketManager(threading.Thread):
    STREAM_URL = 'wss://fstream.binance.com/'

    WEBSOCKET_DEPTH_5 = '5'
    WEBSOCKET_DEPTH_10 = '10'
    WEBSOCKET_DEPTH_20 = '20'

    DEFAULT_USER_TIMEOUT = 30 * 60  # 30 minutes

    def __init__(self, client, user_timeout=DEFAULT_USER_TIMEOUT):
        """Initialise the BinanceSocketManager

        :param client: Binance API client
        :type client: binance.Client
        :param user_timeout: Custom websocket timeout
        :type user_timeout: int

        """
        threading.Thread.__init__(self)
        self._conns = {}
        self._client = client
        self._user_timeout = user_timeout
        self._timers = {'futures': None}
        self._listen_keys = {'futures': None}
        self._account_callbacks = {'futures': None}

    def _start_socket(self, path, callback, prefix='ws/'):
        if path in self._conns:
            return False

        factory_url = self.STREAM_URL + prefix + path
        factory = BinanceFuturesClientFactory(factory_url)
        factory.protocol = BinanceFuturesClientProtocol
        factory.callback = callback
        factory.reconnect = True
        context_factory = ssl.ClientContextFactory()

        self._conns[path] = connectWS(factory, context_factory)
        return path

    def start_depth_socket(self, symbol, callback, depth=None):
        socket_name = symbol.lower() + '@depth'
        if depth and depth != '1':
            socket_name = '{}{}'.format(socket_name, depth)
        return self._start_socket(socket_name, callback)

    def start_kline_socket(self, symbol, callback, interval=Client.KLINE_INTERVAL_1MINUTE):
        socket_name = '{}@kline_{}'.format(symbol.lower(), interval)
        return self._start_socket(socket_name, callback)

    def start_miniticker_socket(self, callback, update_time=1000):
        return self._start_socket('!miniTicker@arr@{}ms'.format(update_time), callback)

    def start_trade_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@trade', callback)

    def start_aggtrade_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@aggTrade', callback)

    def start_symbol_ticker_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@ticker', callback)

    def start_ticker_socket(self, callback):
        return self._start_socket('!ticker@arr', callback)

    def start_symbol_book_ticker_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@bookTicker', callback)

    def start_book_ticker_socket(self, callback):
        return self._start_socket('!bookTicker', callback)

    def start_multiplex_socket(self, streams, callback):
        stream_path = 'streams={}'.format('/'.join(streams))
        return self._start_socket(stream_path, callback, 'stream?')

    def start_futures_user_socket(self, callback):
        # Get the user listen key
        user_listen_key = self._client.futures_stream_get_listen_key()
        # and start the socket with this specific key
        return self._start_account_socket('futures', user_listen_key, callback)

    def _start_account_socket(self, socket_type, listen_key, callback):
        """Starts one of user or margin socket"""
        self._check_account_socket_open(listen_key)
        self._listen_keys[socket_type] = listen_key
        self._account_callbacks[socket_type] = callback
        conn_key = self._start_socket(listen_key, callback)
        if conn_key:
            # start timer to keep socket alive
            self._start_socket_timer(socket_type)
        return conn_key

    def _check_account_socket_open(self, listen_key):
        if not listen_key:
            return
        for conn_key in self._conns:
            if len(conn_key) >= 60 and conn_key[:60] == listen_key:
                self.stop_socket(conn_key)
                break

    def _start_socket_timer(self, socket_type):
        callback = self._keepalive_account_socket

        self._timers[socket_type] = threading.Timer(self._user_timeout, callback, [socket_type])
        self._timers[socket_type].setDaemon(True)
        self._timers[socket_type].start()

    def _keepalive_account_socket(self, socket_type):
        listen_key_func = self._client.futures_stream_get_listen_key
        callback = self._account_callbacks[socket_type]
        listen_key = listen_key_func()
        if listen_key != self._listen_keys[socket_type]:
            self._start_account_socket(socket_type, listen_key, callback)

    def stop_socket(self, conn_key):
        """Stop a websocket given the connection key

        :param conn_key: Socket connection key
        :type conn_key: string

        :returns: connection key string if successful, False otherwise
        """
        if conn_key not in self._conns:
            return

        # disable reconnecting if we are closing
        self._conns[conn_key].factory = WebSocketClientFactory(self.STREAM_URL + 'tmp_path')
        self._conns[conn_key].disconnect()
        del (self._conns[conn_key])

        # check if we have a user stream socket
        if len(conn_key) >= 60 and conn_key[:60] == self._listen_keys['futures']:
            self._stop_account_socket('futures')

    def _stop_account_socket(self, socket_type):
        if not self._listen_keys[socket_type]:
            return
        if self._timers[socket_type]:
            self._timers[socket_type].cancel()
            self._timers[socket_type] = None
        self._listen_keys[socket_type] = None

    def run(self):
        try:
            reactor.run(installSignalHandlers=False)
        except ReactorAlreadyRunning:
            # Ignore error about reactor already running
            pass

    def close(self):
        """Close all connections

        """
        keys = set(self._conns.keys())
        for key in keys:
            self.stop_socket(key)

        self._conns = {}
