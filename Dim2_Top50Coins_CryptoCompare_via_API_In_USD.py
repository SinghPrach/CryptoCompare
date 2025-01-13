from google.colab import drive
drive.mount('/content/drive')
# drive.flush_and_unmount()

from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

now = datetime.now()
current_month_name = now.strftime('%B')
current_year = now.year
current_year_str = str(current_year)
curr_month_year = current_month_name + current_year_str
day = str(now.day)
current_month = current_month_name[:3]

print(current_month)

day_var = day+'_'+curr_month_year

import os
# os.listdir('/content/drive/MyDrive/')

# CryptoCompareAPI
api_key = 'NOT GONNA TELL YOU'

import requests
import time

# Endpoint to get the list of all available cryptocurrencies
url = "https://min-api.cryptocompare.com/data/all/coinlist"

params = {
    'api_key': api_key
}

# Send the request to the CryptoCompare API
response = requests.get(url, params=params)

if response.status_code == 200:
    coins = response.json()['Data']

    # Filter active coins (where IsTrading == True)
    active_coins = [coin for coin in coins.values() if coin['IsTrading']] # list

    # Sort the active coins based on TotalCoinsSupply (in descending order)
    sorted_coins = sorted(active_coins, key=lambda x: x['TotalCoinsMined'] if x.get('TotalCoinsMined') else 0, reverse=True)

df_sortedcoins = pd.DataFrame(sorted_coins)

top_50_coins = df_sortedcoins.sort_values(by='TotalCoinsMined', ascending=False).head(50)
df_top_50_coins = top_50_coins[['Symbol','FullName','TotalCoinsMined']]

df_top_50_coins['ScientificNotation_TotalCoinsMined'] = df_top_50_coins['TotalCoinsMined'].apply(lambda x: '{:e}'.format(x))

df_save = df_top_50_coins[['Symbol','FullName','ScientificNotation_TotalCoinsMined']]

drive_folder = '/content/drive/My Drive/mycolab_container/Top50Coins/'+current_year_str+'/'+current_month+'/'+day+'/'

os.makedirs(drive_folder, exist_ok=True)

parquet_file_path = os.path.join(drive_folder, 'crypto_data.parquet')
df_save.to_parquet(parquet_file_path, engine='pyarrow')

parquet_file_path = drive_folder+'crypto_data.parquet'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(parquet_file_path, engine='pyarrow')

