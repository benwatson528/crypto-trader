from datetime import datetime
import pandas as pd

df = pd.read_csv('historic_bitcoin.zip')
df['Timestamp'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in df['Timestamp']]

print(df)
