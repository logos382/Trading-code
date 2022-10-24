import asyncio
import pandas as pd
import sqlalchemy
import time
from binance import AsyncClient, BinanceSocketManager

starttime = time.time()

async def strategy (symbol):
    pass

async def main(symbol, runtime):
    """An asyncronous fanction that listen for asset market data from the Binance Websocket and execute a strategy

    Args:
        symbol (String): the ticket symbol of the Binance asset we want to retrive.
        runtime (integer): an integer representing the number of seconds we listen to websocket before we close the connection
                           e.g. 60 = 1 minute, 3600 = 1 hour, 86400 = 1 day, 2592000 = 30 Days, 31536000 = 1 year
    """
    # initialise the client
    client = await AsyncClient.create()
    # initialise websocket factory manager
    bsm = BinanceSocketManager(client)
    # create listener using async with
    # this will exit and close the connection after 5 messages
    # start any sockets here, i.e a trade socket
    async with bsm.trade_socket(symbol) as ts:
        # save start and end current time into variables
        currenttime = time.time()
        # Start a while loop with base case
        while currenttime < starttime + runtime:
            # save the received Websocket msg into a variable
            msg = await ts.recv()
            # call a coroutine to excute the writing into sql wile waiting for next msg
            # the attempt here is to continue to listen websocket while writing data
            loop.call_soon(asyncio.create_task, strategy(symbol))
            # update the current time to evaluate loop continuation
            currenttime = time.time()
    # exit the context manager
    await client.close_connection()

if __name__ == "__main__":


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main('BTCBUSD', 36000))