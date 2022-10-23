import asyncio
import pandas as pd
import sqlalchemy
from binance import AsyncClient, BinanceSocketManager

engine = sqlalchemy.create_engine('sqlite:///Trading-code/sqldb/B_Crypto.db')

async def createdf(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:,['s', 'E', 'p']]
    df.columns = ['Symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    print(df)
    return df

async def writesql(msg, symbol):
    frame = await createdf(msg)
    frame.to_sql(symbol, engine, if_exists='append', index=False)

async def main(n, symbol):
    # initialise the client
    client = await AsyncClient.create()
    # initialise websocket factory manager
    bsm = BinanceSocketManager(client)
    # create listener using async with
    # this will exit and close the connection after 5 messages
    # start any sockets here, i.e a trade socket
    async with bsm.trade_socket(symbol) as ts:
        for _ in range(n):
            msg = await ts.recv()
            #print(msg)
            loop.call_soon(asyncio.create_task, writesql(msg, symbol))
    # exit the context manager
    await client.close_connection()

if __name__ == "__main__":

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(5, 'BTCBUSD'))

