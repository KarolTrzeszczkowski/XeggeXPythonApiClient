import json
import time
import hashlib, hmac
try:
    from urllib import urlencode
except:
    from urllib.parse import urlencode
import string
import random
from time import time
import asyncio
import aiohttp
from aiohttp import ClientWebSocketResponse
from functools import wraps
from decimal import Decimal
from typing import Optional, List

class Auth():
    """Authentication class that produces headers for api and login message for websocket."""
    def __init__(self, access_key: str, secret_key: str):
        super().__init__()
        self.secret_key = secret_key
        self.access_key = access_key

    def sign(self, payload: str) -> str:
        """Produces signature for an arbitrary string."""
        signature = hmac.new(
            self.secret_key.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature

    def headers(self, payload: str) -> dict:
        """Creates auth headers for rest API."""
        nonce = str(int(time()*1000))
        signature = self.sign(self.access_key+payload+nonce)
        headers =  {
            "X-API-KEY": self.access_key,
            "X-API-NONCE": nonce,
            "X-API-SIGN": signature,
            "Content-Type": "application/json",
        }
        return headers

    def ws_auth_message(self):
        nonce = ''.join(random.choice(string.ascii_letters+string.digits) for i in range(20))
        return json.dumps({
            'method':'login',
            'params':{
                'algo': "HS256",
                'pKey': self.access_key,
                'nonce': nonce,
                'signature': self.sign(nonce)
            }
        })

def private(func):
    @wraps(func)
    async def wrap(*args, **kwargs):
        assert args[0].auth is not None, "Auth not found. You can't use Account endpoints without specifying API keys. Specify \"access_key\" and \"secret_key\" in \"xeggex_settings.json\" file."
        return await func(*args, **kwargs)
    return wrap

class XeggeXClient():
    def __init__(self):
        try:
            with open('xeggex_settings.json') as f:
                settings = json.load(f)
            self.auth = Auth(settings['access_key'], settings['secret_key'])
        except (FileNotFoundError, KeyError) as ex:
            self.auth = None
        self.endpoint = "https://xeggex.com/api/v2"
        self.ws_endpoint = 'wss://ws.xeggex.com'
        self.session = aiohttp.ClientSession()

    async def get(self, path, params={}):
        params_str = '?'+urlencode(params) if params else ''
        async with self.session.get(
            self.endpoint+path+params_str,
            headers = self.auth.headers(self.endpoint+path+params_str) if self.auth else {}
        ) as resp:
            response = await resp.json()
        return response

    async def post(self, path, data):
        data_str = json.dumps(data,separators=(',',':'))
        async with self.session.post(
            self.endpoint+path,
            data=data_str,
            headers = self.auth.headers(self.endpoint+path+data_str)
        ) as resp:
            response = await resp.json()
            return response

    def websocket_context(self):
        '''Get an entry point to a websocket, to be used with \"async with\"'''
        return self.session.ws_connect(self.ws_endpoint)

    async def ws_stream_generator(self, ws, message, response_methods):
        await ws.send_str(message)
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                notification = msg.json()
                if 'method' in notification.keys():
                    if notification['method'] in response_methods:
                        yield notification
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print(f"websocket connection closed with error {ws.exception()}")
                return
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                print(f"websocket connection closed.")
                return

# Websocket methods

    @private
    async def ws_login(self, ws):
        await ws.send_str(self.auth.ws_auth_message)
        msg = await ws.receive()
        return msg.json()

    @private
    async def ws_create_order(
        self,
        ws,
        symbol,
        side,
        quantity,
        price = None,
        order_type = None,
        user_provided_id = None,
        strict_validate = None,
    ):
        message = {
            'method': 'newOrder',
            'params': {
                'symbol':symbol,
                'side': side,
                'quantity': quantity,
                'price': price,
                'type': order_type,
                'userProvidedId': user_provided_id,
                'strictValidate': strict_validate
            }
        }
        params = message['params'].items()
        for key, value in params:
            if message['params'][key] is None:
                message['params'].pop(key)
        await ws.send_str(json.dumps(message))
        msg = await ws.receive()
        return msg.json()

    @private
    async def ws_cancel_order(self, ws, order_id = None, user_provided_id = None):
        message = {'method': 'cancelOrder',
                   'params': {'orderId': order_id, 'userProvidedId': user_provided_id}}
        error_msg = "You have to unambiguously specify order ID to cancel it"
        assert order_id is not None ^ user_provided_id is not None, error_msg 
        if message['params']['orderId'] is None:
            message['params'].pop('orderId')
        if message['params']['userProvidedId'] is None:
            message['params'].pop('userProvidedId')
        await ws.send_str(json.dumps(message))
        msg = await ws.receive()
        return msg.json()

    @private
    async def ws_get_active_orders(self, ws, symbol = None):
        message = {'method': 'getOrders', 'params': {'symbol': symbol}}
        if message['params']['symbol'] is None:
            message['params'].pop('symbol')
        await ws.send_str(json.dumps(message))
        msg = await ws.receive()
        return msg.json()

    @private
    async def ws_get_trading_balance(self, ws):
        message = json.dumps({'method':'getTradingBalance', 'params':{}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    async def ws_get_assets_list(self, ws):
        message = json.dumps({'method':'getAssets', 'params':{}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    async def ws_get_asset(self, ws, ticker):
        message = json.dumps({'method':'getAssets', 'params':{'ticker': ticker}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()


    async def ws_get_market_list(self, ws):
        message = json.dumps({'method':'getMarkets', 'params':{}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    async def ws_get_market(self, ws, symbol):
        message = json.dumps({'method':'getMarket', 'params': {'symbol': symbol}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    async def ws_get_trade_history(
        self,
        ws,
        symbol,
        limit=None,
        offset=None,
        sort=None,
        history_from=None,
        history_till=None
    ):
        message = {
            'method': 'getTrades',
            'params': {
                'symbol': symbol,
                'limit': limit,
                'offset': offset,
                'sort': sort,
                'from': history_from,
                'till': history_till
            }
        }
        items = message['params'].items()
        for key, value in items:
            if value is None:
                message['params'].pop(key)
        await ws.send_str(json.dumps(message))
        msg = await ws.receive()
        return msg.json()

# Public streams


    def subscribe_ticker_generator(self, ws, symbol):
        message = json.dumps({'method': 'subscribeTicker', 'params': {'symbol': symbol}})
        return self.ws_stream_generator(ws, message, ['ticker'])

    async def unsubscribe_ticker(self, ws, symbol):
        message = json.dumps({'method': 'unsubscribeTicker', 'params': {'symbol': symbol}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    def subscribe_orderbook_generator(self, ws, symbol, limit=None):
        message = {'method': 'subscribeOrderbook', 'params': {'symbol': symbol,'limit': limit}}
        if message['params']['limit'] is None:
            message['params'].pop('limit')
        return self.ws_stream_generator(
            ws, json.dumps(message), ['snapshotOrderbook', 'updateOrderbook'])

    async def unsubscribe_orderbook(self, ws, symbol):
        message = json.dumps({'method': 'unsubscribeOrderbook', 'params': {'symbol': symbol}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    def subscribe_trades_generator(self,ws, symbol):
        message = json.dumps({'method': 'subscribeTrades', 'params': {'symbol': symbol}})
        return self.ws_stream_generator(ws, message, ['snapshotTrades', 'updateTrades'])
        
    async def unsubscribe_trades(self, ws, symbol):
        message = json.dumps({'method': 'unsubscribeTrades', 'params': {'symbol': symbol}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

    def subscribe_candles_generator(self, ws, symbol, period, limit = None):
        message = {'method': 'subscribeCandles',
                   'params': {'symbol':symbol, 'period': period, 'limit': limit}}
        if message['params']['limit'] is None:
            message['params'].pop('limit')
        return self.ws_stream_generator(
            ws, json.dumps(message), ['snapshotCandles', 'updateCandles'])

    async def unsubscribe_candles(self, ws, symbol, period):
        message = json.dumps({'method': 'unsubscribeCandles',
                              'params': {'symbol': symbol, 'period':period}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()


# Private streams

    @private
    def subscribe_reports_generator(self, ws):
        message = json.dumps({'method': 'subscribeReports', 'params': {}})
        return self.ws_stream_generator(ws, message)

    @private
    async def unsubscribe_reports(self, ws):
        message = json.dumps({'method': 'subscribeReports', 'params': {}})
        await ws.send_str(message)
        msg = await ws.receive()
        return msg.json()

# Public methods

    async def get_assets(self):
        path = '/asset/getlist'
        return await self.get(path)

    async def get_asset_by_id(self, asset_id):
        path = f'/asset/getbyid/{asset_id}'
        return await self.get(path)

    async def get_asset_by_id(self, ticker):
        path = f'/asset/getbyticker/{ticker}'
        return await self.get(path)

    async def get_markets(self):
        path = '/market/getlist'
        return await self.get(path)

    async def get_market_by_id(self, market_id):
        path = f'/market/getbyid/{market_id}'
        return await self.get(path)

    async def get_market_by_symbol(self, symbol):
        path = f'/market/getbyid/{symbol}'
        return await self.get(path)

    async def get_pools(self):
        path = '/pool/getlist'
        return await self.get(path)

    async def get_pool_by_id(pool_id):
        path = f'/pool/getbyid/{pool_id}'
        return await self.get(path)

    async def get_pool_by_id(pool_symbol):
        path = f'/pool/getbysymbol/{pool_symbol}'
        return await self.get(path)

    async def get_orderbook_by_symbol(symbol):
        path = f'/market/getorderbookbysymbol/{symbol}'
        return await self.get(path)

    async def get_orderbook_by_market_id(market_id):
        path = f'/market/getorderbookbymarketid/{market_id}'
        return await self.get(path)

# Private methods

    @private
    async def get_balances(self):
        path = '/balances'
        return await self.get(path)

    @private
    async def get_nonzero_balances(self):
        bal = await self.get_balances()
        return [
            b for b in bal
            if Decimal(b['available'])!=0 or Decimal(b['pending'])!=0 or Decimal(b['held'])!=0
        ]

    @private
    async def get_deposit_address(self, ticker):
        path = f'/getdepositaddress/{ticker}'
        return await self.get(path)

    @private
    async def create_order(
        self,
        symbol,
        side,
        quantity,
        price=None,
        order_type=None,
        user_provided_id=None,
        strict_validate=None,
    ):
        path = '/createorder'
        data =  {
            'userProvidedId': user_provided_id,
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': volume,
            'price': price,
            'strictValidate': strict_validate,
        }
        items = data.items()
        if order_type in [None, 'limit']:
            assert price is not None, "Specify price for a limit order"
        for key, value in items:
            if value is None:
                data.pop(key)
        return await self.post(path, data)

    @private
    async def cancel_order(self, order_id):
        path = '/cancel_order'
        data = {'id': order_id}
        return self.post(path, data)

    @private
    async def cancel_market_orders(self, symbol, side):
        path = '/cancelallorders'
        data = {
            'symbol': symbol,
            'side': side
        }
        return await self.post(path, data)

    @private
    async def create_withdrawal(self, ticker, quantity, address, payment_id=None):
        path = '/createwithdrawal'
        data = {
            "ticker": ticker,
            "quantity": quantity,
            "address": address,
            "paymentId": payment_id
        }
        if not payment_id:
            data.pop('paymentId')
        return self.post(path, data)

    @private
    async def get_deposits(self, limit, skip, ticker=None):
        path = '/getdeposits'
        params = { 'ticker': ticker, 'limit': limit, 'skip':skip }
        if ticker is None:
            params.pop('ticker')
        return self.get(path, params)

    @private
    async def get_withdrawals(self, limit, skip, ticker=None):
        path = '/getwithdrawals'
        params = { 'ticker': ticker, 'limit': limit, 'skip':skip }
        if ticker is None:
            params.pop('ticker')
        return self.get(path, params)

    @private
    async def get_order(self, order_id):
        path = f'/getorder/{order_id}'
        return await self.get(path)

    @private
    async def get_my_orders(self, status, limit, skip, symbol=None):
        path = '/getorders'
        params = {
            'symbol': symbol,
            'status':status,
            'limit': limit,
            'skip': skip
        }
        if symbol is None:
            params.pop('symbol')
        return await self.get(path, params)

    @private
    async def get_trades(self, limit, skip, symbol=None):
        path = '/gettrades'
        params = {'limit':limit, 'skip':skip, 'symbol':symbol}
        if symbol is None:
            params.pop('symbol')
        return await self.get(path, params)

    @private
    async def get_trades_since(self, since, limit, skip, symbol=None):
        path = '/gettradesince'
        params = {'since':since, 'limit':limit, 'skip':skip, 'symbol':symbol}
        if symbol is None:
            params.pop('symbol')
        return await self.get(path, params)

    @private
    async def get_pool_trades(self, limit, skip, symbol=None):
        path = '/getpooltrades'
        params = {'limit':limit, 'skip':skip, 'symbol':symbol}
        if symbol is None:
            params.pop('symbol')
        return await self.get(path, params)

    @private
    async def get_pool_trades_since(self, since, limit, skip, symbol=None):
        path = '/getpooltradessince'
        params = {'since':since, 'limit':limit, 'skip':skip, 'symbol':symbol}
        if symbol is None:
            params.pop('symbol')
        return await self.get(path, params)

