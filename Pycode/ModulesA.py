from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy import inspect
import pandas as pd

def use_inspector(conn):
    inspector = inspect(conn)
    return inspector.get_table_names()

class AlertManager():

    def __init__(self, asyncengine):
        self.alert_dict = {}
        self.engine = asyncengine

    async def async_manage_alerts(self, tablenames):
        for tablename, alertprices in tablenames.items():
            if tablename in self.alert_dict:
                alert = self.alert_dict[tablename]
                updated_alert = await alert.async_check_alert(tablename, self.engine)
                self.alert_dict[tablename] = updated_alert
            else:
                alert = Alert(alertprices[0], alertprices[1])
                self.alert_dict[tablename] = alert


class Alert():

    def __init__(self, priceup, pricedown):
        self.priceup = priceup
        self.pricedown = pricedown
        self.alertpricebreakup = 0
        self.alertpricecloseup = 0
        self.alertpriceholdsup = 0

    async def async_check_alert(self, tablename, asyncengine):
        async with asyncengine.connect() as asynconn:
            tables = await asynconn.run_sync(use_inspector)
            if tablename in tables:
                query = text(f'SELECT * FROM {tablename}')
                df = await asynconn.run_sync(DataOperator.read_sql, query)
                df['CloseTime'] = pd.to_datetime(df['CloseTime'])
                df = df.set_index(['CloseTime'])
                df5min = df['LastPrice'].resample('5min').agg(Open="first", Close="last", High="max", Low="min")
                df5min['Symbol'] = tablename
                currentclose = df5min['Close'].iloc[-1:].values
                lastclose = df5min['Close'].iloc[-2:-1].values
                previousclose = df5min['Close'].iloc[-3:-2].values
                print('====================================')
                print(f'this is alert for {tablename}, {self.priceup}, {self.pricedown}')
                print(f'{currentclose}, {lastclose}, {previousclose}')
                if self.alertpricebreakup == 0:
                    if currentclose > self.priceup:
                        print('Price went above alert')
                        self.alertpricebreakup = 1
                    else:
                        print('Waiting for breakup')
                if self.alertpricebreakup == 1 and self.alertpricecloseup == 0:
                    if lastclose > self.priceup:
                        print('price closed above the alert') 
                        self.alertpricecloseup = 1
                    else:
                        print('waiting to closeup')
                if self.alertpricebreakup == 1 and self.alertpricecloseup == 1 and self.alertpriceholdsup == 0:
                    if previousclose > self.priceup:
                        print('price holds above alert') 
                        self.alertpriceholdsup = 1
                    else:
                        print('waiting to holdup')
                print('====================================')
            return self


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

    async def async_create_df(self, msg):
        df = pd.DataFrame([msg[0]])
        df = df.loc[:, ['timestamp', 'symbol', 'price', 'amount', 'id']].rename(columns={"timestamp": "CloseTime", "price": "LastPrice"})
        df.LastPrice = df.LastPrice.astype(float)
        df.CloseTime = pd.to_datetime(df.CloseTime, unit='ms')
        return df

    async def async_write_sql(self, msg, symbol, exchange):
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
            tables = await conn.run_sync(use_inspector)
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
