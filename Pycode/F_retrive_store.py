# import ccxt.async_support as ccxt   # noqa: E402
# import asyncio
# import time

# ftx = ccxt.ftx()
# markets = ['BTC/USDT']

# async def main(symbol, exchange, runtime):
#     starttime = time.time()
#     currenttime = time.time()
#     while currenttime < starttime + runtime:
#         msg = await exchange.fetch_tickers(symbol)
#         currenttime = time.time()
#         print(msg)
#     await exchange.close()

# if __name__ == "__main__":


#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main(markets, ftx, 60))

import requests
import websocket
import json
import pandas as pd

_END_POINT = 'wss://ftx.com/ws/'

subscribe_msg = json.dumps({'op': 'subscribe', 'channel': 'ticker', 'market': 'BTC-PERP'})

def on_open(ws):
    ws.send(subscribe_msg)

def on_message(ws, message):
    out = json.loads(message)
    df_ = pd.DataFrame(out['data'], index=[0])
    df_.index = pd.to_datetime(df_.time, unit='s')
    print(df_[['bid', 'ask', 'last']])
    

ws = websocket.WebSocketApp(_END_POINT, on_message=on_message, on_open=on_open)

ws.run_forever()





