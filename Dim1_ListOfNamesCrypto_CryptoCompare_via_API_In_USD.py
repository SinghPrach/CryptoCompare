from google.colab import drive
drive.mount('/content/drive')

import os
# os.listdir('/content/drive/MyDrive/')

# CryptoCompareAPI
api_key = 'NOT GONNA SHARE :)'

import requests
import time
import pandas as pd
from datetime import datetime

# Function to fetch the full coin list (symbols and full names) - took 2 sec
def get_crypto_full_names(api_key):
    url = 'https://min-api.cryptocompare.com/data/all/coinlist'
    headers = {
        'Authorization': f'Apikey {api_key}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data['Data']
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

# Fetch full crypto names and symbols
crypto_list = get_crypto_full_names(api_key)

print(type(crypto_list))

print(len(crypto_list))

print(len(crypto_list)) # 16071

# Convert the dictionary into a DataFrame
dim_names_crypto_df = pd.DataFrame([(symbol, details['CoinName']) for symbol, details in crypto_list.items()], columns=['Symbol', 'Full Name'])

dim_names_crypto_df['SerialNumber']=range(1, len(dim_names_crypto_df) + 1)

dim_names_crypto_df.columns = dim_names_crypto_df.columns.str.replace(" ","_")

dim_names_crypto_df = dim_names_crypto_df[['SerialNumber','Symbol','Full_Name']]

dim_names_crypto_df['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

dim_names_crypto_df['Timestamp'] = pd.to_datetime(dim_names_crypto_df['Timestamp'])

dim_names_crypto_df['Date'] = dim_names_crypto_df['Timestamp'].dt.date

dim_names_crypto_df['Month'] = pd.to_datetime(dim_names_crypto_df['Date']).dt.month
dim_names_crypto_df['Year'] = pd.to_datetime(dim_names_crypto_df['Date']).dt.year
dim_names_crypto_df['Month_Name'] = pd.to_datetime(dim_names_crypto_df['Date']).dt.month_name()

current_month_name = datetime.now().strftime('%B')
current_year = datetime.now().year
curr_month_year = current_month_name + str(current_year)

# Define the path where the file will be saved
drive_folder = '/content/drive/My Drive/colab_container/crypto_data/Dim/NamesSymbolCrypto/'+curr_month_year

# Create the folder if it doesn't exist
os.makedirs(drive_folder, exist_ok=True)

import pyarrow as pa
import pyarrow.parquet as pq

# Convert DataFrame to Parquet and save it to Google Drive
parquet_file_path = os.path.join(drive_folder, 'crypto_data.parquet')
dim_names_crypto_df.to_parquet(parquet_file_path, engine='pyarrow')

print(f"File saved to: {parquet_file_path}")

# Reading the data we just wrote
parquet_file_path = drive_folder+'crypto_data.parquet'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(parquet_file_path, engine='pyarrow')
