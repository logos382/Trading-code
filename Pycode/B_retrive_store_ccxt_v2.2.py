import asyncio
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy import inspect
import time
import ccxt.async_support as ccxt

# create the engine to write/read into the sql database(e.g. an sqlite db)
engine = sqlalchemy.create_engine('sqlite:///Trading-code/Sqldb/B_Crypto.db', poolclass=sqlalchemy.pool.QueuePool)
Session = sessionmaker(bind=engine)

Symbols = ['BTC_USDT', 'RUNE_USDT']

async def createdf(msg):
    """_summary_

    Args:
        msg (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Create a Data Frame from websocket msg
    df = pd.DataFrame([msg[0]])
    print(df)
    # Slice Dataframe to keep only required data
    df = df.loc[:,['symbol', 'timestamp', 'price']].rename(columns={"timestamp": "CloseTime", "price": "LastPrice"})
    # Convert Text to float for future calculations
    df.LastPrice = df.LastPrice.astype(float)
    # convert unix UTC time to more readable one
    df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
    # print(df)
    return df


async def writesql(msg, symbol):
    """_summary_

    Args:
        msg (_type_): _description_
        symbol (_type_): _description_
    """
    # Call createdf() to create DataFrame from message
    frame = await createdf(msg)
    insp = inspect(engine)
    if insp.has_table(symbol):
        df = pd.read_sql('SELECT CloseTime, LastPrice FROM '+ symbol, con=engine)
        lastrow = df.tail(1)
        if lastrow["LastPrice"].values != frame["LastPrice"].values:
            session = Session()  # Create a session manually
            # Perform batch insert within a transaction 
            transaction = session.begin()
            try:
                frame.to_sql(symbol, engine, if_exists='append', index=False)
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
            frame.to_sql(symbol, engine, if_exists='append', index=False)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(f'Error: {e}')
            # Proper error handling implementation...

async def main(symbol, runtime):
    """An asyncronous fanction that listen for asset market data from the Binance Websocket and store the data into an sql database

    Args:
        symbol (String): the ticket symbol of the Binance asset we want to retrive and store data,
        runtime (integer): an integer representing the number of seconds we listen to websocket before we close the connection
                           e.g. 60 = 1 minute, 3600 = 1 hour, 86400 = 1 day, 2592000 = 30 Days, 31536000 = 1 year
    """
    exchange = ccxt.mexc()
    exchange.enableRateLimit = True

    starttime = time.time()
    
    while True:
        for symbol in Symbols:
            try:
                msg = await exchange.fetch_trades(symbol, limit=1)
                # call a coroutine to excute the writing process into sql while waiting for next msg
                # the attempt here is to continue to listen websocket while writing data
                loop.call_soon(asyncio.create_task, writesql(msg, symbol))
                # print(msg[0], '\n')
            except ccxt.RequestTimeout as e:
                print('[' + type(e).__name__ + ']')
                print(str(e)[0:200])
                 # will retry
            except ccxt.DDoSProtection as e:
                print('[' + type(e).__name__ + ']')
                print(str(e.args)[0:200])
                # will retry
            except ccxt.ExchangeNotAvailable as e:
                print('[' + type(e).__name__ + ']')
                print(str(e.args)[0:200])
                # will retry
            except ccxt.ExchangeError as e:
                print('[' + type(e).__name__ + ']')
                print(str(e)[0:200])
                break  # won't retry
        
        currenttime = time.time()
        if currenttime >= starttime + runtime:
            break

    await exchange.close()






if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(Symbols, 6000))

