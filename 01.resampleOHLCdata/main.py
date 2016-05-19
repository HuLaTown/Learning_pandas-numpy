from datetime import datetime
import pandas as pd

df = pd.read_table('000002.txt', names=['tradedate', 'minute', 'high', 'low', 'close', 'volume'])

df.loc[:, 'tradedate'] = [str(x) for x in df.loc[:, 'tradedate']]
df.loc[:, 'minute'] = ['0'+str(x) if len(str(x)) < 4 else str(x) for x in df.loc[:, 'minute']]
df.insert(0, 'datetime', df.tradedate+df.minute)

df.loc[:, 'datetime'] = [datetime.strptime(x, '%Y%m%d%H%M') for x in df.loc[:, 'datetime']]

df = df.drop(df.columns[[1, 2]], 1)

df.insert(1, 'open', df.close)  # insert test data of open price since real open price data is missing
# need to be removed after open price added

ohlc_dict = {
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last',
    'Volume': 'sum'
    }

df = df.set_index(['datetime'])

df.resample('5min', how=ohlc_dict, label='right', closed='right')
