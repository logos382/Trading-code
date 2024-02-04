# sqlalchemy 2.0.21
# pandas 2.1.1

import asyncio
from asyncio import gather, run
from sqlalchemy.ext.asyncio import create_async_engine
import time
import ccxt.async_support as ccxt
from Allertmanager import AlertManager
from Dataoperator import DataOperator
from Pyrobot import Robot

RUNTIME_SECONDS = 3600*1 # 60 = 1m; 3600 = 1H; 86700 = 1D; 607800 = 1W

exchanges = {
        'mexc': ['BTC/USDT', 'RUNE/USDT'],
        'binance': ['BTC/USDT', 'RUNE/USDT'],

    }

tablenames = { 
    'BTC_USDT_mexc' : (43230,33800),
    'RUNE_USDT_mexc' : (5.66, 2.70),
    'BTC_USDT_binance' : (43230,33800),
    'RUNE_USDT_binance' : (5.66, 2.70)

    }  


# create the engine to write/read into the sql database(e.g. an sqlite db)
asyncengine = Robot.create_asyncengine('sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db')
manager = AlertManager(asyncengine)
dataoperator = DataOperator(asyncengine)

async def async_symbol_loop(exchange, symbol):
    print(f'Starting the {exchange.id} loop with {symbol}')
    end_time = time.time() + RUNTIME_SECONDS
    while time.time() < end_time:
        try:
            msg = await exchange.fetch_trades(symbol, limit=1)
            await gather(dataoperator.async_to_sql(msg, symbol, exchange), manager.async_manage_alerts(tablenames))
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


async def background_task(task_queue):
        coroloops = [async_exchange_loop(exchange_id, symbols) for exchange_id, symbols in exchanges.items()]
        othercoro = []
        await gather(*coroloops, *othercoro)


async def main(exchanges):
        task_queue = asyncio.Queue()
        background_task_handle = asyncio.create_task(background_task(task_queue))
        while True:
            user_choice_task = asyncio.create_task(Robot.user_input())

            # Wait for either user input or background task completion
            done, pending = await asyncio.wait(
                [user_choice_task,background_task_handle],
                return_when=asyncio.FIRST_COMPLETED
            )
            # Handle user choice
            if user_choice_task in done:
                user_choice_result = user_choice_task.result()
                print(user_choice_result)
                task_queue.put_nowait(user_choice_result)

                # Handle background task completion
            #if background_task_handle in done:
            # Do any additional processing if needed
            #        pass
            
# def main():
#     robot = Robot()
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(robot.run())
#     loop.close()


if __name__ == "__main__":
    run(main(exchanges))


