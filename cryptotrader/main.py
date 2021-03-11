import time
from datetime import datetime

import pandas as pd

start_time = time.time()

pd.set_option('display.max_columns', None)
df = pd.read_csv('historic_bitcoin.zip')

np = df.to_numpy()

np.