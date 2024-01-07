# sqlalchemy 2.0.21
# pandas 2.1.1

import asyncio
from asyncio import gather, run
from sqlalchemy.ext.asyncio import create_async_engine
import time
import ccxt.async_support as ccxt
from ModulesA import allertmanager, allert, dataio

# create the engine to write/read into the sql database(e.g. an sqlite db)
asyncengine = create_async_engine(
    'sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db',
    #pool_size=5,  # Adjust the pool size based on your needs
    #max_overflow=0  # Adjust the max_overflow based on your needs
)

START_TIME = time.time()
RUNTIME_SECONDS = 3600*3 # 60 = 1m; 3600 = 1H; 86700 = 1D; 607800 = 1W

exchanges = {
        'mexc': ['BTC/USDT', 'RUNE/USDT'],
        'binance': ['BTC/USDT', 'RUNE/USDT'],

    }

tablenames = { 
    'BTC_USDT_MEXCGlobal' : (43230,33800),
    'RUNE_USDT_MEXCGlobal' : (5.66, 2.70),
    'BTC_USDT_Binance' : (43230,33800),
    'RUNE_USDT_Binance' : (5.66, 2.70)

    }  

manager = allertmanager(asyncengine)
dataoperator = dataio(asyncengine)

async def async_symbol_loop(exchange, symbol, RUNTIME_SECONDS):
    print(f'Starting the {exchange.id} loop with {symbol}')
    while True:
        try:
            msg = await exchange.fetch_trades(symbol, limit=1)
            # ticker = await exchange.fetch_ticker(symbol)
            # print(exchange.iso8601(exchange.milliseconds()), 'fetched', symbol, 'ticker from', exchange.name)
            # print (msg[0])
            # print(ticker)
            # print (msg[0]['datetime'],msg[0]['price'])
            # print(ticker['datetime'],ticker['close'],ticker['bid'])
            # await gather (dataoperator.async_write_sql(msg, symbol, exchange), dataoperator.async_read_sql(tablenames),manager.async_manage_allerts(tablenames))
            await gather (dataoperator.async_write_sql(msg, symbol, exchange), manager.async_manage_allerts(tablenames))
        except ccxt.RequestTimeout as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            # will retry
        except ccxt.DDoSProtection as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
           #  will retry
        except ccxt.ExchangeNotAvailable as e:
            print('[' + type(e).__name__ + ']')
            print(str(e.args)[0:200])
            # will retry
        except ccxt.ExchangeError as e:
            print('[' + type(e).__name__ + ']')
            print(str(e)[0:200])
            # raise e  # uncomment to break all loops in case of an error in any one of them
            break  # you can break just this one loop if it fails# won't retry
        currenttime = time.time()
        if currenttime >= START_TIME + RUNTIME_SECONDS:
            break


async def async_exchange_loop(exchange_id, symbols, RUNTIME_SECONDS):
    print('Starting the', exchange_id, 'exchange loop with', symbols)
    exchange = getattr(ccxt, exchange_id)({
        'options': {
            'defaultType': 'spot',  # future, spot, margin, future, delivery
        },
    })
    exchange.enableRateLimit = True
    coroloops = [async_symbol_loop(exchange, symbol, RUNTIME_SECONDS) for symbol in symbols]
    await gather(*coroloops)
    await exchange.close()


async def main(exchanges, RUNTIME_SECONDS):
        coroloops = [async_exchange_loop(exchange_id, symbols, RUNTIME_SECONDS) for exchange_id, symbols in exchanges.items()]
        await gather(*coroloops)




if __name__ == "__main__":
    run(main(exchanges, RUNTIME_SECONDS))

