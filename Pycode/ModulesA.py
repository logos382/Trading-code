from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

class allertmanager():

    def __init__(self):
        self.allertdict = {}

    async def manageallerts(self, tablenames):
        for tablename, allertprices in tablenames.items():
            if tablename in self.allertdict:
                oallert = self.allertdict[tablename]
                await oallert.c_read_sql(tablename)
            else:
                oallert = allert(allertprices[0], allertprices[1])
                self.allertdict[tablename] = oallert

class allert():

    def __init__(self, priceup, pricedown):
        self.priceup = priceup
        self.pricedown = pricedown
    
    async def c_read_sql(self, tablename):
        print('this is allert', self.priceup, self.pricedown)
        asyncengine = create_async_engine('sqlite+aiosqlite:///Trading-code/Sqldb/B_Crypto.db')
        async with asyncengine.connect() as asynconn:
            query = text('SELECT * FROM '+ tablename)
            df = await asynconn.run_sync(read_sql_aioAlchemy, query)
            df['CloseTime'] = pd.to_datetime(df['CloseTime'])
            df = df.set_index(['CloseTime'])
            df5min = df['LastPrice'].resample('5min').agg(Open="first", Close="last",High="max", Low="min")
            df5min['Symbol'] = tablename
            currentclose = df5min['Close'].iloc[-1:].values
            lastclose = df5min['Close'].iloc[-2:-1].values
            previousclose = df5min['Close'].iloc[-3:-2].values
            print(tablename, currentclose,lastclose,previousclose)
            if currentclose > self.priceup:
                print('Price went above allert')
                if lastclose > self.priceup:
                    print('price closed above the allert') 
                    if previousclose > self.priceup:
                        print('price holds above allert') 
