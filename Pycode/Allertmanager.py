from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from sqlalchemy import inspect
import pandas as pd
from Dataoperator import DataOperator

class AlertManager():

    def __init__(self, asyncengine):
        self.alert_dict = {}
        self.engine = asyncengine

    async def async_manage_alerts(self, tablenames):
        for tablename, alertprices in tablenames.items():
            alert = self.alert_dict.setdefault(tablename, Alert(alertprices[0], alertprices[1]))
            updated_alert = await alert.async_check_alert(tablename, self.engine)
            self.alert_dict[tablename] = updated_alert


class Alert():

    def __init__(self, priceup, pricedown):
        self.priceup = priceup
        self.pricedown = pricedown
        self.alertpricebreakup = 0
        self.alertpricecloseup = 0
        self.alertpriceholdsup = 0

    async def async_check_alert(self, tablename, asyncengine):
        async with asyncengine.connect() as asynconn:
            tables = await asynconn.run_sync(DataOperator.use_inspector)
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
                # print('====================================')
                # print(f'this is alert for {tablename}, {self.priceup}, {self.pricedown}')
                # print(f'{currentclose}, {lastclose}, {previousclose}')
                if self.alertpricebreakup == 0:
                    if currentclose > self.priceup:
                        # print('Price went above alert')
                        self.alertpricebreakup = 1
                    else:
                        pass
                        # print('Waiting for breakup')
                if self.alertpricebreakup == 1 and self.alertpricecloseup == 0:
                    if lastclose > self.priceup:
                        # print('price closed above the alert') 
                        self.alertpricecloseup = 1
                    else:
                        pass
                        # print('waiting to closeup')
                if self.alertpricebreakup == 1 and self.alertpricecloseup == 1 and self.alertpriceholdsup == 0:
                    if previousclose > self.priceup:
                        # print('price holds above alert') 
                        self.alertpriceholdsup = 1
                    else:
                        pass
                #         print('waiting to holdup')
                # print('====================================')
            return self


