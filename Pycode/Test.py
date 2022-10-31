import time
import pandas as pd

time0 = time.time() 
time1 = time.gmtime(time0)
time2 = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
dindex = pd.to_datetime(time0, unit='s')
print(time0,time1,time2, dindex)
