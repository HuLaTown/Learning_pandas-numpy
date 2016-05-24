from datetime import datetime
import pandas as pd

df = pd.read_table('000002.txt', names=['tradedate', 'minute', 'high', 'low', 'close', 'volume'])

df.insert(2, 'open', df.close.shift(1))  # copy OPEN by the previous CLOSE

df.loc[:, 'tradedate'] = [str(x) for x in df.loc[:, 'tradedate']]
df.loc[:, 'minute'] = ['0'+str(x) if len(str(x)) < 4 else str(x) for x in df.loc[:, 'minute']]
df.insert(0, 'datetime', df.tradedate+df.minute)

df.loc[:, 'datetime'] = [datetime.strptime(x, '%Y%m%d%H%M') for x in df.loc[:, 'datetime']]

df = df.drop(df.columns[[1, 2]], 1)


# df.insert(1, 'open', df.close)  # insert test data of open price since real open price data is missing
# need to be removed after open price added


ohlc_dict = {
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum'
    }

df = df.set_index(['datetime'])


# TODO get all trade dates
# pd.PeriodIndex(df.index,freq='D').unique()

# TODO foreach trade date, resample am & pm data according to the bin size
# df[df.index < datetime(tradedates[0].strftime('%Y'), tradedates[0].strftime('%m'), tradedates[0].strftime('%d'),11,30,00)]
# y =


# TODO merge them all(append)


df.resample('5min', how=ohlc_dict, label='right', closed='right')

# bin_size = 5


# TODO prevent NAN data beyond the trading time
# TODO shift the resampled data in order to show the price of 10:00
# TODO align data such as 11:30
