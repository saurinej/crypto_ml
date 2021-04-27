import pandas as pd
import ccxt
import crypto_config
import os


def fetch_data(symbol, min_interval, points):
    # Enter Binance keys
    exchange = ccxt.binanceus({
        'apiKey': crypto_config.binance_codes['key'],
        'secret': crypto_config.binance_codes['secret']
    })

    # Load markets
    markets = exchange.load_markets()
    print('Markets loaded.')

    # Prepare arguments for data retrieval with ccxt library
    tf = str(min_interval) + 'm'
    if points > 1000:
        points = 1000

    # Get Cardano data, 5/15min interval, train for past 6 months/180 days/4,320 hours/259,200 minutes/51,840 5min
    # intervals or 17,280 15min intervals
    ada_data_raw = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=points)
    print('Data obtained for {0} at {1} minute intervals'.format(symbol, min_interval))
    # Convert to pandas data frame
    ada_data_pandas = pd.DataFrame(ada_data_raw, columns=['unix_time', 'c_open', 'high', 'low', 'c_close', 'volume'])

    return ada_data_pandas
