from datetime import datetime
import pandas as pd
import numpy as np
import sys


def data_process(bin_size, source_file_path, result_file_path):
    df = pd.read_table(source_file_path, names=['tradedate', 'minute', 'high', 'low', 'close', 'volume'])

    df.insert(2, 'open', df.close.shift(1))  # copy OPEN by the previous CLOSE

    df.loc[:, 'tradedate'] = [str(x) for x in df.loc[:, 'tradedate']]
    df.loc[:, 'minute'] = ['0'+str(x) if len(str(x)) < 4 else str(x) for x in df.loc[:, 'minute']]
    df.insert(0, 'datetime', df.tradedate+df.minute)

    df.loc[:, 'datetime'] = [datetime.strptime(x, '%Y%m%d%H%M') for x in df.loc[:, 'datetime']]

    df = df.drop(df.columns[[1, 2]], 1)

    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
        }

    df = df.set_index(['datetime'])
    tradedate_list = pd.PeriodIndex(df.index, freq='D').unique()

    shift_am = 30 % int(bin_size)
    shift_pm = -(60 % int(bin_size))

    # TODO foreach trade date, resample am & pm data according to the bin size
    df_todo = df
    df_result = pd.DataFrame()
    for tradedate in tradedate_list:
        tradedate_year  = int(tradedate.strftime('%Y'))
        tradedate_month = int(tradedate.strftime('%m'))
        tradedate_day   = int(tradedate.strftime('%d'))
        # for AM
        df_todo = df_todo[df_todo.index >= datetime(tradedate_year, tradedate_month, tradedate_day, 9, 30, 0)]
        dftmp   = df_todo[df_todo.index <= datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30, 0)]

        dftmp   = dftmp.resample(bin_size+'min', how=ohlc_dict, label='right', closed='right')
        # align the "11:30" data.
        # (1) save the outsider's quotation data
        open  = \
            dftmp[dftmp.index == datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30+shift_am, 0)].open
        high  = \
            dftmp[dftmp.index == datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30+shift_am, 0)].high
        low   = \
            dftmp[dftmp.index == datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30+shift_am, 0)].low
        close = \
            dftmp[dftmp.index == datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30+shift_am, 0)].close
        volume = \
            dftmp[dftmp.index == datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30+shift_am, 0)].volume
        if bin_size != '1' or shift_am > 0:
            # (2) remove the outsider
            dftmp = dftmp[dftmp.index != datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30 + shift_am, 0)]
            # (3) add back with "11:30"
            dftmp.loc[datetime(tradedate_year, tradedate_month, tradedate_day, 11, 30, 0),
                  ['open', 'high', 'low', 'close', 'volume']] = np.array([open, high, low, close, volume]).reshape(5)

        # append AM data to the result data frame
        df_result = df_result.append(dftmp)

        # for PM
        df_todo = df_todo[df_todo.index >= datetime(int(tradedate_year), int(tradedate_month), int(tradedate_day), 13, 0, 0)]
        dftmp   = df_todo[df_todo.index <= datetime(int(tradedate_year), int(tradedate_month), int(tradedate_day), 15, 0, 0)]

        dftmp   = dftmp.resample(bin_size+'min', how=ohlc_dict, label='right', closed='right', base=shift_pm)
        # append PM data to the result data frame
        df_result = df_result.append(dftmp)

    df_result = df_result[['open', 'high', 'close', 'low', 'volume']]
    df_result.to_csv(result_file_path)


def main(argv):
    if len(argv) < 4:
        print "Not enough arguments. Usage: main.py <bin size> <source file path> <result file path>"
        sys.exit(2)
    elif len(argv) > 4:
        print len(argv)
        print "Too many arguments. Usage: main.py <bin size> <source file path> <result file path>"
        sys.exit(2)
    else:
        bin_size = str(argv[1])
        source_file_path = str(argv[2])
        result_file_path = str(argv[3])

        if bin_size not in ['1', '2', '3', '4', '5', '6', '8', '10', '12', '15', '20', '30', '60']:
            print "Bin size argument must in \"1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 30, 60\""
            sys.exit(2)

        data_process(bin_size, source_file_path, result_file_path)

if __name__ == "__main__":
    main(sys.argv)
