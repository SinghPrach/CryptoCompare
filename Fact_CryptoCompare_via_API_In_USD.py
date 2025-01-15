from google.colab import drive
drive.mount('/content/drive')
# drive.flush_and_unmount()

from datetime import datetime
import pandas as pd

now = datetime.now()
current_month_name = now.strftime('%B')
current_year = now.year
current_year_str = str(current_year)
curr_month_year = current_month_name + current_year_str
day = str(now.day)
current_month = current_month_name[:3]

drive_folder_top50coins = '/content/drive/My Drive/mycolab_container/Top50Coins/'+current_year_str+'/'+current_month+'/'+day+'/'

parquet_top50coins = drive_folder_top50coins+'crypto_data.parquet'

# Read the Parquet file into a DataFrame
df_top_50_coins = pd.read_parquet(parquet_top50coins, engine='pyarrow')

crypto_symbols = df_top_50_coins['Symbol'].tolist()

import os
# os.listdir('/content/drive/MyDrive/')

# CryptoCompareAPI
api_key = 'NOT GONNA SHARE HERE:))' 

import requests
import time

# Endpoint to get the list of all available cryptocurrencies
url = "https://min-api.cryptocompare.com/data/all/coinlist"

params = {
    'api_key': api_key
}

# Send the request to the CryptoCompare API
response = requests.get(url, params=params)

# Get the list of all available cryptocurrencies
# crypto_symbols = ['BTC', 'ETH', 'XRP', 'LTC', 'ADA', 'SOL', 'DOGE', 'BNB', 'DOT', 'SHIB', 'MATIC', 'AVAX', 'TRX', 'DOGE']

# Define the target currencies
# target_currencies = 'USD,EUR'
target_currencies = 'USD'

# Function to get detailed data for a list of cryptocurrencies using API key
def get_full_data(cryptos, target_currencies, api_key):
    # Join the list into a string separated by commas
    fsyms = ','.join(cryptos)

    # Define parameters for the request
    params = {
        'fsyms': fsyms,  # List of cryptocurrencies (e.g., BTC,ETH)
        'tsyms': target_currencies,  # Target currencies (e.g., USD,EUR)
        'api_key': api_key  # Your API key
    }

    # Send the GET request
    response = requests.get('https://min-api.cryptocompare.com/data/pricemultifull', params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()  # Parse the JSON response
        return data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def fetch_all_crypto_full_data_columns():
    all_data = []
    batch_size = 50
    for i in range(0, len(crypto_symbols), batch_size):
        batch = crypto_symbols[i:i+batch_size]
        print(f"Fetching detailed data for: {batch}")
        data = get_full_data(batch, target_currencies, api_key)
        if data:
            for crypto_symbol, details in data['RAW'].items():
                for currency, stats in details.items():
                    row = {
                        'Crypto': crypto_symbol,
                        'Currency': currency,
                        'Price': stats['PRICE'],
                        'Market Cap': stats['MKTCAP']
                    }
                    all_data.append(row)
        time.sleep(1)  # Adjust sleep time based on your rate limit and API tier
    return all_data

data_all_cols = fetch_all_crypto_full_data_columns()

# df = pd.DataFrame(all_data)
import pandas as pd
df_50Coins = pd.DataFrame(data_all_cols)

df_50Coins['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

df_50Coins['Day'] = day
df_50Coins['Month'] = current_month
df_50Coins['Year'] = current_year

## Path where data is updated daily
path_daily_data = '/content/drive/My Drive/mycolab_container/Fact/USD_DailyData/'+current_year_str+'/'+current_month+'/'+day+'/'

os.makedirs(path_daily_data, exist_ok=True)

parquet_path_daily_data = os.path.join(path_daily_data, 'crypto_daily_data.parquet')
df_50Coins.to_parquet(parquet_path_daily_data, engine='pyarrow')


## Historical Data Path
path_historical_data = '/content/drive/My Drive/mycolab_container/Fact/USD_HistoricalData/'

# Using Pyspark session for appending data as the python solution is not economical
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Append Parquet") \
    .getOrCreate()

df_50Coins_spark = spark.createDataFrame(df_50Coins)

df_50Coins_spark.write \
    .mode("append") \
    .parquet(path_historical_data)

df_test = spark.read.parquet(path_historical_data)
