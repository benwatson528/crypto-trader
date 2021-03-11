from datetime import datetime

import pandas as pd

# Re-creates the historic_bitcoin input file with manipulations applied; saves about 30s per run.
pd.set_option('display.max_columns', None)
df = pd.read_csv('../raw_bitcoin_history.csv.zip')

df.dropna(inplace=True)
df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
df['percentage_change'] = df['Open'].sub(df['Open'].shift()).div(df['Open'] - 1).fillna(0)

df.rename(columns={'Timestamp': 'timestamp',
                   'Open': 'open',
                   'High': 'high',
                   'Low': 'low',
                   'Close': 'close',
                   'Volume_(BTC)': 'volume_btc',
                   'Volume_(Currency)': 'volume_currency',
                   'Weighted_Price': 'weighted_price'},
          inplace=True)

df.to_parquet('historic_bitcoin.parquet', engine='fastparquet', compression='GZIP')
