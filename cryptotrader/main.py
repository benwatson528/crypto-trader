import time

import numpy as np
import pandas as pd

FEE = 0.001  # 0.1%
BUY_STATE = 'LOOKING_TO_BUY'
SELL_STATE = 'LOOKING_TO_SELL'

# pd.set_option('display.max_columns', None)
df = pd.read_parquet('historic_bitcoin.parquet')
a = df[['timestamp', 'weighted_price']].to_numpy()


def trade(data, sell_threshold, buy_threshold, large_sell_threshold, large_buy_threshold):
    # Assume that we've started by putting $1000 in BTC
    btc_price_at_buy = df['weighted_price'].iloc[0]
    btc_held = 1000.00 / df['weighted_price'].iloc[0]
    initial_btc_held = btc_held
    cash_wallet = 0.00
    current_state = SELL_STATE
    btc_price_at_sell = 0.00
    start = time.time()
    row_num = 0
    num_txns = 0
    num_large_buys = 0
    num_small_buys = 0
    num_sells = 0
    total_fee_paid = 0.00
    fee_this_period = 0.00
    is_large_buy = False
    previous_cash_wallet = 1000.00
    for row in data:
        timestamp = row[0]
        current_btc_price = row[1]
        # if row_num % 100000 == 0:
        #     print(f'Current state: {current_state}')
        #     print(f'Row number = {row_num:,}, Date = {timestamp}, BTC price = ${current_btc_price:,.2f}')
        #     print(f'Expected return if HODLing = ${(initial_btc_held * current_btc_price):,.2f}')
        #     if current_state == SELL_STATE:
        #         print(f'Cash value of held Bitcoins = ${(btc_held * current_btc_price):,.2f}')
        #         print(f'Bitcoin bought at ${btc_price_at_buy:,.2f}')
        #     else:
        #         print(f'Intermediate cash wallet = ${cash_wallet:,.2f}')
        #     print(
        #         f'Intermediate fee paid = ${total_fee_paid:,.2f}, ${total_fee_paid - fee_this_period:,.2f} this period')
        #     print(f'Intermediate BTC held = {btc_held:,.2f}')
        #     print(f'Intermediate num transactions = {num_txns:,}')
        #     print('-------------------')
        #     fee_this_period = total_fee_paid
        row_num += 1
        if current_state == SELL_STATE:
            current_sell_threshold = large_sell_threshold if is_large_buy else sell_threshold
            if current_btc_price >= (btc_price_at_buy + (btc_price_at_buy * current_sell_threshold)):
                sell_fee_paid = btc_held * FEE
                total_fee_paid += sell_fee_paid
                # Multiply to get from BTC to cash
                cash_wallet = (btc_held - sell_fee_paid) * current_btc_price
                if cash_wallet <= previous_cash_wallet:
                    print("We lost money on the last trade")
                btc_price_at_sell = current_btc_price
                btc_held = 0.00
                current_state = BUY_STATE
                num_sells += 1
                num_txns += 1
        elif current_state == BUY_STATE:
            if current_btc_price <= (btc_price_at_sell - (btc_price_at_sell * buy_threshold)):
                buy_fee_paid = cash_wallet * FEE
                total_fee_paid += buy_fee_paid
                # Divide to get from cash to BTC
                btc_held = (cash_wallet - buy_fee_paid) / current_btc_price
                btc_price_at_buy = current_btc_price
                previous_cash_wallet = cash_wallet
                cash_wallet = 0.00
                current_state = SELL_STATE
                num_small_buys += 1
                num_txns += 1
                is_large_buy = False
            elif current_btc_price >= (btc_price_at_sell + (btc_price_at_sell * large_buy_threshold)):
                buy_fee_paid = cash_wallet * FEE
                total_fee_paid += buy_fee_paid
                # Divide to get from cash to BTC
                btc_held = (cash_wallet - buy_fee_paid) / current_btc_price
                btc_price_at_buy = current_btc_price
                previous_cash_wallet = cash_wallet
                cash_wallet = 0.00
                current_state = SELL_STATE
                num_large_buys += 1
                num_txns += 1
                is_large_buy = True

    print('\n')
    print(f'Runtime = {time.time() - start}')
    print(f'Time period = {data[0][0]} to {data[-1][0]}')
    print(f'Final cash wallet = ${cash_wallet:,.2f}')
    print(f'Total fee paid = ${total_fee_paid:,.2f}')
    print(f'Final BTC held = {btc_held:,.2f}')
    print(f'Num transactions = {num_txns}')
    print(f'Num large buys = {num_large_buys}')
    print(f'Num small buys = {num_small_buys}')
    print(f'Num sells = {num_sells}')
    print(f'Total fee paid = {num_sells}')

    return btc_held, cash_wallet


split = np.array_split(a, 10)

for arr in split:
    end_btc, end_cash = trade(arr, 0.03, 0.01, 0.03, 0.1)
