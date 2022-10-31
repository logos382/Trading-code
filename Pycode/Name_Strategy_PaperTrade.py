import asyncio
import pandas as pd
import sqlalchemy
import time
from binance import AsyncClient, BinanceSocketManager

# define strategy constant
pair = 'BTCBUSD'
entry_ret = -0.00013
qnty = 1
lookback = 60

# create the engine to write/read into the sql database(e.g. an sqlite db)
engine = sqlalchemy.create_engine('sqlite:///Trading-code/sqldb/B_Crypto.db')

async def writetrade():
    """_summary_
    """
    # TO DO
    pass

async def strategy (symbol, entry_ret, lookback, qnty, msg, open_position=False):
    """_summary_

    Args:
        symbol (_type_): _description_
        entry_ret (_type_): _description_
        lookback (_type_): _description_
        qnty (_type_): _description_
        open_position (bool, optional): _description_. Defaults to False.
    """
    # Starting a endless loop
    while True:
        print('waiting to execute Buy')
        # load the DB into Pandas
        df = pd.read_sql(symbol, engine)
        # slice the df for the perios
        lookback_period = df.iloc[-lookback:]
        # calculate cumulative returns
        cumret = (lookback_period.Price.pct_change() + 1).cumprod()-1
        ret = cumret[cumret.last_valid_index()]
        print(ret)
        if cumret[cumret.last_valid_index()] < entry_ret:
            # TO DO write the paper Buy order into the DB
            b_order_time = time.time()
            b2_order_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(b_order_time))
            b_order_price = await price(msg)
            print(b2_order_time, b_order_price)
            open_position = True
            break
    # Now coding the trailing stop 
    if open_position:
        while True:
            print('waiting to execute Sell')
            # from the sql database slect only prices occurred affter the order time
            """ ++++ note that in the paper trade Buy Oreder time is capturred with time.time() which return the UNIX
            time in seconds hence the unit='s' parameters, the websocket msg provide the UNIX time in millisecond hence unit='ms'
            +++++ """
            df = pd.read_sql(f"""SELECT * FROM {symbol} WHERE Time >= \
            '{pd.to_datetime(b_order_time, unit='s')}'""", engine)
            df['Benchmark'] = df.Price.cummax()
            df['TSL'] = df.Benchmark * 0.998
            if df[df.Price < df.TSL].first_valid_index():
                # TO DO write the paper Buy order into the DB
                s_order_time = time.time()
                s2_order_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(s_order_time))
                s_order_price = await price(msg)
                print(s2_order_time, s_order_price)
                break 

async def price(msg):
    """_summary_

    Args:
        msg (_type_): _description_

    Returns:
        _type_: _description_
    """
    live_price = float(msg['p'])
    live_time = msg['E']
    print(live_price)
    return live_price

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
        starttime = time.time()
        currenttime = time.time()
        # Start a while loop with base case
        while currenttime < starttime + runtime:
            # save the received Websocket msg into a variable
            msg = await ts.recv()
            # call a coroutine to excute the writing into sql wile waiting for next msg
            # the attempt here is to continue to listen websocket while writing data
            loop.call_soon(asyncio.create_task, strategy (symbol, entry_ret, lookback, qnty, msg, open_position=False))
            # update the current time to evaluate loop continuation
            currenttime = time.time()
    # exit the context manager
    await client.close_connection()

if __name__ == "__main__":


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(pair, 10))