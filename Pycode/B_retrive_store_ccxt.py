import asyncio
import pandas as pd
import sqlalchemy
import time
import ccxt.async_support as ccxt

# create the engine to write/read into the sql database(e.g. an sqlite db)
engine = sqlalchemy.create_engine('sqlite:///Trading-code/Sqldb/B_Crypto.db')

symbol = 'BTCBUSD'


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
    df = df.loc[:,['symbol', 'closeTime', 'lastPrice']]
    # Convert Text to float for future calculations
    df.lastPrice = df.lastPrice.astype(float)
    # convert unix UTC time to more readable one
    df.closeTime = pd.to_datetime(df.closeTime, unit='ms')
    print(df)
    return df

async def writesql(msg, symbol):
    """_summary_

    Args:
        msg (_type_): _description_
        symbol (_type_): _description_
    """
    # save the Dataframe with Websocket msg nto a variable
    frame = await createdf(msg)
    # write the Dataframe into sqlite database
    frame.to_sql(symbol, engine, if_exists='append', index=False)

async def main(symbol, runtime):
    """An asyncronous fanction that listen for asset market data from the Binance Websocket and store the data into an sql database

    Args:
        symbol (String): the ticket symbol of the Binance asset we want to retrive and store data,
        runtime (integer): an integer representing the number of seconds we listen to websocket before we close the connection
                           e.g. 60 = 1 minute, 3600 = 1 hour, 86400 = 1 day, 2592000 = 30 Days, 31536000 = 1 year
    """
    exchange = ccxt.binance()
    exchange.enableRateLimit = True  # enable
    # save start and end current time into variables
    starttime = time.time()
    currenttime = time.time()
    # Start a while loop with base case
    while currenttime < starttime + runtime:
        try:
            # save the received Websocket msg into a variable
            msg = await exchange.fetch_ticker(symbol)
#            print(exchange.iso8601(exchange.milliseconds()), 'fetched', symbol, 'ticker from', exchange.name)
            print(msg)
            # call a coroutine to excute the writing process into sql while waiting for next msg
            # the attempt here is to continue to listen websocket while writing data
            loop.call_soon(asyncio.create_task, writesql(msg, symbol))
            # update the current time to evaluate loop continuation
            currenttime = time.time()
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
    # exit the context manager
    await exchange.close()


if __name__ == "__main__":


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbol, 60))

