from asyncio import gather, run, sleep
import ccxt.async_support as ccxt  # noqa: E402
import asyncio
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
import time

# create the engine to write/read into the sql database(e.g. an sqlite db)
engine = sqlalchemy.create_engine('sqlite:///Trading-code/Sqldb/B_Crypto.db', poolclass=sqlalchemy.pool.QueuePool)
Session = sessionmaker(bind=engine)

starttime = time.time()
runtime = 600 # 60 = 1m; 3600 = 1H; 86700 = 1D; 607800 = 1W

exchanges = {
        'mexc': ['BTC/USDT', 'RUNE/USDT'],
    }

tablenames = ['RUNE_USDT_MEXCGlobal']

async def createdf(msg):
    """_summary_

    Args:
        msg (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Create a Data Frame from websocket msg
    df = pd.DataFrame([msg[0]])
    # print(df)
    # Slice Dataframe to keep only required data
    df = df.loc[:,['timestamp', 'symbol', 'price','amount','id']].rename(columns={"timestamp": "CloseTime", "price": "LastPrice"})
    # Convert Text to float for future calculations
    df.LastPrice = df.LastPrice.astype(float)
    # convert unix UTC time to more readable one
    df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
    #print(df)
    return df


async def writesql(msg, symbol, exchange):
    """_summary_

    Args:
        msg (_type_): _description_
        symbol (_type_): _description_
    """
    # Call createdf() to create DataFrame from message
    frame = await createdf(msg)
    insp = inspect(engine)
    tablename = symbol.replace("/", "_")+"_"+str(exchange).replace(" ", "")
    if insp.has_table(tablename):
        df = pd.read_sql('SELECT id FROM '+ tablename, con=engine)
        lastrow = df.tail(10)
        if frame["id"].values not in lastrow["id"].values :
            session = Session()  # Create a session manually
            # Perform batch insert within a transaction 
            transaction = session.begin()
            try:
                frame.to_sql(tablename, engine, if_exists='append', index=False)
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                print(f'Error: {e}')
                # Proper error handling implementation...  
    else:
        session = Session()  # Create a session manually
        # Perform batch insert within a transaction 
        transaction = session.begin()
        try:
            frame.to_sql(tablename, engine, if_exists='append', index=False)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f'Error: {e}')
            # Proper error handling implementation...


async def readsql(tablenames, runtime):
    while True:
        insp = inspect(engine)
        for tablename in tablenames:
            if insp.has_table(tablename):
                df = pd.read_sql('SELECT * FROM '+ tablename, con=engine)
                lastrow = df.tail(1)
                # print(lastrow, 'readsql')
                await sleep(5)
        currenttime = time.time()
        if currenttime >= starttime + runtime:
            break


async def symbol_loop(exchange, symbol, runtime):
    print('Starting the', exchange.id, 'symbol loop with', symbol)
    while True:
        try:
            msg = await exchange.fetch_trades(symbol, limit=1)
            now = exchange.milliseconds()
            # print(exchange.iso8601(now), exchange.id, symbol, orderbook['asks'][0], orderbook['bids'][0])
            print(exchange.iso8601(now), exchange.id, symbol, msg)
            await writesql(msg, symbol, exchange)
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
    loops = [symbol_loop(exchange, symbol, runtime) for symbol in symbols]
    await gather(*loops)
    await exchange.close()


async def main(exchanges, runtime, tablenames):
        loops = [exchange_loop(exchange_id, symbols, runtime) for exchange_id, symbols in exchanges.items()]
        # await gather(*loops,readsql(tablenames, runtime))
        await gather(*loops)



if __name__ == "__main__":
    run(main(exchanges, runtime, tablenames))