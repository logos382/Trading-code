from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy import inspect
import pandas as pd

class DataOperator():

    def __init__(self, asyncengine, buffer_size: int = 109):
        self.engine = asyncengine
        self.buffer = None
        self.BUFFER_SIZE = buffer_size

    @staticmethod
    def to_sql(conn, frame, tablename):
        frame.to_sql(tablename, conn, if_exists='append', index=False)

    @staticmethod
    def read_sql(conn, query):
        return pd.read_sql(query, conn)
    
    @staticmethod
    def use_inspector(conn):
        inspector = inspect(conn)
        return inspector.get_table_names()

    async def async_create_df(self, msg):
        df = pd.DataFrame([msg[0]])
        df = df.loc[:, ['timestamp', 'symbol', 'price', 'amount', 'id']].rename(columns={"timestamp": "CloseTime", "price": "LastPrice"})
        df.LastPrice = df.LastPrice.astype(float)
        df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
        return df

    async def async_to_sql(self, msg, symbol, exchange):
        frame = await self.async_create_df(msg)
        frame['tablename'] = f"{symbol.replace('/', '_')}_{exchange.id.replace(' ', '')}"
        if not isinstance(self.buffer, pd.DataFrame):
            self.buffer = frame
        elif len(self.buffer) < self.BUFFER_SIZE:
            if frame["id"].values not in self.buffer["id"].values:
                self.buffer = pd.concat([self.buffer, frame], ignore_index=True)
        elif frame["id"].values not in self.buffer["id"].values:
            self.buffer = pd.concat([self.buffer, frame], ignore_index=True)
            try:
                async with self.engine.begin() as conn:
                    series = self.buffer['tablename'].drop_duplicates()
                    for tablename in series:
                        rslt_df = self.buffer[self.buffer['tablename'] == tablename].iloc[:, :5]
                        # print(f'Wrote on sql table {tablename} {len(rslt_df)} items')
                        await conn.run_sync(DataOperator.to_sql, rslt_df, tablename)        
            except Exception as e:
                print(f'Error: {e}')
            self.buffer = self.buffer.tail(10)
        return None

    async def async_read_sql(self, tablenames):
        async with self.engine.connect() as conn:
            tables = await conn.run_sync(DataOperator.use_inspector)
            for tablename, alertprices in tablenames.items():
                if tablename in tables:
                    query = text(f'SELECT * FROM {tablename}')
                    df = await conn.run_sync(DataOperator.read_sql, query)
                    df['CloseTime'] = pd.to_datetime(df['CloseTime'])
                    df = df.set_index(['CloseTime'])
                    dfxmin = df['LastPrice'].resample('5min').agg(Open="first", Close="last", High="max", Low="min")
                    dfxmin['Symbol'] = tablename
                    print(f'this is dataio for {tablename}')
            return None
