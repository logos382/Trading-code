from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy import inspect
import pandas as pd

def use_inspector(conn):
    inspector = inspect(conn)
    # return any value to the caller
    return inspector.get_table_names()

def to_sql_aioAlchemy(conn, frame, tablename):
    frame.to_sql(tablename, conn, if_exists='append', index=False)
    return None

def read_sql_aioAlchemy( conn, query):
    df = pd.read_sql(query, conn)
    return df


class AllertManager():

    def __init__(self, asyncengine):
        self.allertdict = {}
        self.engine = asyncengine

    async def async_manage_allerts(self, tablenames):
        for tablename, allertprices in tablenames.items():
            if tablename in self.allertdict:
                oallert = self.allertdict[tablename]
                noallert = await oallert.async_check_allert(tablename, self.engine)
                self.allertdict[tablename] = noallert
            else:
                oallert = Allert(allertprices[0], allertprices[1])
                self.allertdict[tablename] = oallert


class Allert():

    def __init__(self, priceup, pricedown):
        self.priceup = priceup
        self.pricedown = pricedown
        self.allertpricebreakup = 0
        self.allertpricecloseup = 0
        self.allertpriceholdsup = 0
    
    async def async_check_allert(self, tablename, asyncengine):
        async with asyncengine.connect() as asynconn:
            tables = await asynconn.run_sync(use_inspector)
            if tablename in tables:
                query = text('SELECT * FROM '+ tablename)
                df = await asynconn.run_sync(read_sql_aioAlchemy,query)
                df['CloseTime'] = pd.to_datetime(df['CloseTime'])
                df = df.set_index(['CloseTime'])
                df5min = df['LastPrice'].resample('5min').agg(Open="first", Close="last",High="max", Low="min")
                df5min['Symbol'] = tablename
                currentclose = df5min['Close'].iloc[-1:].values
                lastclose = df5min['Close'].iloc[-2:-1].values
                previousclose = df5min['Close'].iloc[-3:-2].values
                print('====================================')
                print(f'this is allert for {tablename}, {self.priceup}, {self.pricedown}')
                print(f'{currentclose}, {lastclose}, {previousclose}')
                if self.allertpricebreakup == 0:
                    if currentclose > self.priceup:
                        print('Price went above allert')
                        self.allertpricebreakup = 1
                    else:
                        print('Waiting for breakup')
                if self.allertpricebreakup == 1 and self.allertpricecloseup == 0:
                    if lastclose > self.priceup:
                        print('price closed above the allert') 
                        self.allertpricecloseup = 1
                    else:
                        print('waiting to closeup')
                if self.allertpricebreakup == 1 and self.allertpricecloseup == 1 and self.allertpriceholdsup == 0:
                    if previousclose > self.priceup:
                        print('price holds above allert') 
                        self.allertpriceholdsup = 1
                    else:
                        print('waiting to holdup')
                print('====================================')
            else:
                pass
            return self


class DataIO():
     
    def __init__(self, asyncengine, buffer_size: int = 109)-> None:
        self.engine = asyncengine
        self.buffer = None
        self.BUFFER_SIZE = buffer_size

    async def async_create_df(self,msg):
        """_summary_

        Args:
            msg (_type_): _description_

        Returns:
        _   type_: _description_
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
    
    async def async_write_sql(self, msg, symbol, exchange):
    # Call createdf() to create DataFrame from message
        frame = await self.async_create_df(msg)
        frame['tablename'] = symbol.replace("/", "_")+"_"+str(exchange).replace(" ", "")
        if not isinstance(self.buffer, pd.core.frame.DataFrame):
            self.buffer = frame
            # print(len(self.buffer))
        elif isinstance(self.buffer, pd.core.frame.DataFrame) and len(self.buffer) < self.BUFFER_SIZE:
                if frame["id"].values not in self.buffer["id"].values :
                    self.buffer = pd.concat([self.buffer, frame], ignore_index=True)
                    # print(len(self.buffer))
        elif isinstance(self.buffer, pd.core.frame.DataFrame) and len(self.buffer) == self.BUFFER_SIZE or len(self.buffer) > self.BUFFER_SIZE:
                if frame["id"].values not in self.buffer["id"].values :
                    self.buffer = pd.concat([self.buffer, frame], ignore_index=True)
                    try:
                        async with self.engine.begin() as asynconnbegin:
                            # print(f'Buffer full, writing on sql db')
                            # print(len(self.buffer))
                            series = pd.Series(self.buffer.tablename) 
                            series = series.drop_duplicates()
                            for tablename in series:
                                rslt_df = self.buffer.loc[self.buffer['tablename'] == tablename]
                                rslt_df = rslt_df.iloc[:,:5]
                                print(f'Wrote on sql table {tablename} {len(rslt_df)} items')
                                # print(len(self.buffer))
                                await asynconnbegin.run_sync(to_sql_aioAlchemy, rslt_df, tablename)                                         
                    except Exception as e:
                        print(f'Error: {e}')
                        # Proper error handling implementation...  
                self.buffer = self.buffer.tail(10) #empty the buffer
        else:
            pass
        return None

    async def async_read_sql(self, tablenames):
        async with self.engine.connect() as asynconn:
            tables = await asynconn.run_sync(use_inspector)
            for tablename, allertprices in tablenames.items():
                if tablename in tables:
                    query = text('SELECT * FROM '+ tablename)
                    df = await asynconn.run_sync(read_sql_aioAlchemy, query)
                    df['CloseTime'] = pd.to_datetime(df['CloseTime'])
                    df = df.set_index(['CloseTime'])
                    dfxmin = df['LastPrice'].resample('5min').agg(Open="first", Close="last",High="max", Low="min")
                    dfxmin['Symbol'] = tablename
                    print(f'this is dataio for {tablename}')
            return None #dfxmin            
                    