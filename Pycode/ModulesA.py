
class allertmanager():

    def __init__(self):
        self.allertdict = {}

    async def createallert(self, tablename, priceup, pricedown):
        if tablename in self.allertdict:
            self.allertdict[tablename].c_read_sql(tablename)
        else:
            oallert = allert(priceup, pricedown)
            self.allertdict[tablename] = oallert




class allert():

    def __init__(self, priceup, pricedown):
        self.priceup = priceup
        self.pricedown = pricedown
    
    async def c_read_sql(tablename):
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
            if currentclose > allertprice:
                print('Price went above allert')
                if lastclose > allertprice:
                    print('price closed above the allert') 
                    if previousclose > allertprice:
                        print('price holds above allert') 
