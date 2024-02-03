# sqlalchemy 2.0.21
# pandas 2.1.1

import asyncio
from asyncio import gather, run
from sqlalchemy.ext.asyncio import create_async_engine
import time
import ccxt.async_support as ccxt
from ModulesA import AllertManager, Allert, DataIO
from Pyrobot import Robot

RUNTIME_SECONDS = 60*3 # 60 = 1m; 3600 = 1H; 86700 = 1D; 607800 = 1W

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


# create the engine to write/read into the sql database(e.g. an sqlite db)
asyncengine = Robot.create_asyncengine('sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db')
manager = AllertManager(asyncengine)
dataoperator = DataIO(asyncengine)

async def async_symbol_loop(exchange, symbol):
    print(f'Starting the {exchange.id} loop with {symbol}')
    end_time = time.time() + RUNTIME_SECONDS
    while time.time() < end_time:
        try:
            msg = await exchange.fetch_trades(symbol, limit=1)
            # ticker = await exchange.fetch_ticker(symbol)
            # print(exchange.iso8601(exchange.milliseconds()), 'fetched', symbol, 'ticker from', exchange.name)
            # print (msg[0])
            # print(ticker)
            # print (msg[0]['datetime'],msg[0]['price'])
            # print(ticker['datetime'],ticker['close'],ticker['bid'])
            # await gather (dataoperator.async_write_sql(msg, symbol, exchange), dataoperator.async_read_sql(tablenames),manager.async_manage_allerts(tablenames))
            await gather(dataoperator.async_write_sql(msg, symbol, exchange), manager.async_manage_allerts(tablenames))
        except (ccxt.RequestTimeout, ccxt.DDoSProtection, ccxt.ExchangeNotAvailable) as e:
            print(f'[{type(e).__name__}] {str(e.args)[:200]}')
        except ccxt.ExchangeError as e:
            print(f'[{type(e).__name__}] {str(e)[:200]}')
            break


async def async_exchange_loop(exchange_id, symbols):
    print(f'Starting the {exchange_id} exchange loop with {symbols}')
    exchange = Robot.create_exchanges(exchange_id, 'spot')
    coroloops = [async_symbol_loop(exchange, symbol) for symbol in symbols]
    await gather(*coroloops)
    await exchange.close()


async def main(exchanges):
        coroloops = [async_exchange_loop(exchange_id, symbols) for exchange_id, symbols in exchanges.items()]
        await gather(*coroloops)


if __name__ == "__main__":
    run(main(exchanges))


