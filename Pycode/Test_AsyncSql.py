# sqlalchemy 2.0.21
# pandas 2.1.1

import asyncio
from asyncio import gather, run
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine
import time
import ccxt.async_support as ccxt


# create the engine to write/read into the sql database(e.g. an sqlite db)
asyncengine = create_async_engine('sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db')

starttime = time.time()
runtime = 60 # 60 = 1m; 3600 = 1H; 86700 = 1D; 607800 = 1W

exchanges = {
        'mexc': ['BTC/USDT', 'RUNE/USDT'],
    }

tablenames = ['RUNE_USDT_MEXCGlobal', 'BTC_USDT_MEXCGlobal']

def use_inspector(conn):
    inspector = inspect(conn)
    # return any value to the caller
    return inspector.get_table_names()

def to_sql_aioAlchemy(conn, frame, tablename):
    frame.to_sql(tablename, conn, if_exists='append', index=False)

def read_sql_aioAlchemy(conn, query):
    df = pd.read_sql(query, conn)
    return df


async def createdf(msg):
    """_summary_

    Args:
        msg (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Create a Data Frame from websocket msg
    df = pd.DataFrame([msg[0]])
    # Slice Dataframe to keep only required data
    df = df.loc[:,['timestamp', 'symbol', 'price','amount','id']].rename(columns={"timestamp": "CloseTime", "price": "LastPrice"})
    # Convert Text to float for future calculations
    df.LastPrice = df.LastPrice.astype(float)
    # convert unix UTC time to more readable one
    df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
    return df


async def write_sql(msg, symbol, exchange):
    """_summary_

    Args:
        msg (_type_): _description_
        symbol (_type_): _description_
    """
    # Call createdf() to create DataFrame from message
    frame = await createdf(msg)
    async with asyncengine.connect() as asynconn:
        tables = await asynconn.run_sync(use_inspector)
        tablename = symbol.replace("/", "_")+"_"+str(exchange).replace(" ", "")
        if tablename in tables:
            query = text('SELECT id FROM '+ tablename)
            df = await asynconn.run_sync(read_sql_aioAlchemy, query)
            last10rows = df.tail(10)
            if frame["id"].values not in last10rows["id"].values :
                try:
                    async with asyncengine.begin() as asynconnbegin:
                       print('writing 1')
                       await asynconnbegin.run_sync(to_sql_aioAlchemy, frame, tablename)
                except Exception as e:
                    print(f'Error: {e}')
                    # Proper error handling implementation...  
        else:
            try:
                async with asyncengine.begin() as asynconnbegin:
                    print('writing 2')
                    await asynconnbegin.run_sync(to_sql_aioAlchemy, frame, tablename)
            except Exception as e:
                print(f'Error: {e}')
                # Proper error handling implementation...


async def c_read_sql(tablenames):
    while True:
        async with asyncengine.connect() as asynconn:
            tables = await asynconn.run_sync(use_inspector)
            for tablename in tablenames:
                if tablename in tables:
                    query = text('SELECT * FROM '+ tablename)
                    df = await asynconn.run_sync(read_sql_aioAlchemy, query)
                    df['CloseTime'] = pd.to_datetime(df['CloseTime'])
                    df = df.set_index(['CloseTime'])
                    df5min = df['LastPrice'].resample('5min').agg(Open="first", Close="last",High="max", Low="min")
                    df5min['Symbol'] = tablename
                    print(df5min.tail(1))
            currenttime = time.time()
            if currenttime >= starttime + runtime:
                break
            return df


async def symbol_loop(exchange, symbol, runtime):
    print('Starting the', exchange.id, 'symbol loop with', symbol)
    while True:
        try:
            msg = await exchange.fetch_trades(symbol, limit=1)
            await gather (write_sql(msg, symbol, exchange), c_read_sql(tablenames))
            # await gather (writesql(msg, symbol, exchange))
        except Exception as e:
            print(str(e))
            # raise e  # uncomment to break all loops in case of an error in any one of them
            break  # you can break just this one loop if it fails
        currenttime = time.time()
        if currenttime >= starttime + runtime:
            break


async def exchange_loop(exchange_id, symbols, runtime):
    print('Starting the', exchange_id, 'exchange loop with', symbols)
    exchange = getattr(ccxt, exchange_id)()
    exchange.enableRateLimit = True
    coroloops = [symbol_loop(exchange, symbol, runtime) for symbol in symbols]
    await gather(*coroloops)
    await exchange.close()


async def main(exchanges, runtime):
        coroloops = [exchange_loop(exchange_id, symbols, runtime) for exchange_id, symbols in exchanges.items()]
        await gather(*coroloops)



if __name__ == "__main__":
    run(main(exchanges, runtime))