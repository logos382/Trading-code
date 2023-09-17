import asyncio
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import time
import ccxt.async_support as ccxt
import os
import sys

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

# create the engine to write/read into the sql database(e.g. an sqlite db)
engine = sqlalchemy.create_engine('sqlite:///Trading-code/Sqldb/B_Crypto.db', poolclass=sqlalchemy.pool.QueuePool)
Session = sessionmaker(bind=engine)

Symbols = ['BTC/USDT', 'RUNE/USDT']

async def createdf(msg):
    """_summary_

    Args:
        msg (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Create a Data Frame from websocket msg
    df = pd.DataFrame([msg['info']])
    # Slice Dataframe to keep only required data
    df = df.loc[:,['symbol', 'time', 'last']].rename(columns={"time": "CloseTime", "last": "LastPrice"})
    # Convert Text to float for future calculations
    df.LastPrice = df.LastPrice.astype(float)
    # convert unix UTC time to more readable one
    df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
    return df

async def main(symbol, runtime):
    """An asyncronous fanction that listen for asset market data from the Binance Websocket and store the data into an sql database

    Args:
        symbol (String): the ticket symbol of the Binance asset we want to retrive and store data,
        runtime (integer): an integer representing the number of seconds we listen to websocket before we close the connection
                           e.g. 60 = 1 minute, 3600 = 1 hour, 86400 = 1 day, 2592000 = 30 Days, 31536000 = 1 year
    """
    exchange = ccxt.mexc()
    exchange.enableRateLimit = True

    session = Session()  # Create a session manually
    
    starttime = time.time()
    
    while True:
        for symbol in Symbols:
            try:
                msg = await exchange.fetch_ticker(symbol)
                print(msg)

                # Call createdf() to create DataFrame from message
                frame = await createdf(msg)

                # Perform batch insert within a transaction
                transaction = session.begin()
                try:
                    frame.to_sql(symbol, engine, if_exists='append', index=False)
                    transaction.commit()
                except Exception as e:
                    transaction.rollback()
                    print(f'Error: {e}')
                    # Proper error handling implementation...

            except Exception as e:
                print(f'Error: {e}')
                # Proper error handling implementation...

        currenttime = time.time()
        if currenttime >= starttime + runtime:
            break

    await exchange.close()


if __name__ == "__main__":


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(Symbols, 60))

