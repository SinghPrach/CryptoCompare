from google.colab import drive
drive.mount('/content/drive')
# drive.flush_and_unmount()

# Using Pyspark session as Spark makes joining and similar stuffs easier
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Append Parquet") \
    .getOrCreate()

from datetime import datetime

now = datetime.now()
current_month_name = now.strftime('%B')
current_year = now.year
current_year_str = str(current_year)
curr_month_year = current_month_name + current_year_str
day = str(now.day)
current_month = current_month_name[:3]

import os
# os.listdir('/content/drive/MyDrive/')

import pandas as pd

drive_folder_top50coins = '/content/drive/My Drive/mycolab_container/Top50Coins/'+current_year_str+'/'+current_month+'/'+day+'/'
parquet_top50coins = drive_folder_top50coins+'crypto_data.parquet'

path_daily_data = '/content/drive/My Drive/mycolab_container/Fact/USD_DailyData/'+current_year_str+'/'+current_month+'/'+day+'/'
parquet_path_daily_data = os.path.join(path_daily_data, 'crypto_daily_data.parquet')

path_historical_data = '/content/drive/My Drive/mycolab_container/Fact/USD_HistoricalData/'

df_top_50_coins = pd.read_parquet(parquet_top50coins, engine='pyarrow')
df_daily_crypto_data = pd.read_parquet(parquet_path_daily_data, engine='pyarrow')
df_historical_crypto_data = spark.read.parquet(path_historical_data)

# Converting the pandas dataframe to spark df
spark_df_top_50_coins = spark.createDataFrame(df_top_50_coins)
spark_df_daily_crypto_data = spark.createDataFrame(df_daily_crypto_data)

# Importing required pyspark modules
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# For daily data dashboarding, our left table is spark_df_daily_crypto_data
# Keeping spark_df_daily_crypto_data as left table, we will join with the spark_df_top_50_coins to get 
# FullName and ScientificNotation_TotalCoinsMined for the corresponding crypto.

df_getFullName_TotalCoinsMined = spark_df_daily_crypto_data.alias("a").join(spark_df_top_50_coins.alias("b"),(col("a.Crypto")==col("b.Symbol")),"left")\
                                  .select(spark_df_daily_crypto_data["*"],spark_df_top_50_coins["FullName"],spark_df_top_50_coins["ScientificNotation_TotalCoinsMined"])\
                                  .drop(spark_df_daily_crypto_data["Timestamp"])

def clean_column_names(df):
    new_columns = [col.strip().replace(" ", "_").replace("!", "").replace("#", "") for col in df.columns]
    return df.toDF(*new_columns)

# Clean the column names
df_dailyFact_cleanColNames = clean_column_names(df_getFullName_TotalCoinsMined)

# Cleaning the column names
df_dailyFact_columnsRenamed = df_dailyFact_cleanColNames.withColumnRenamed("FullName", "Crypto_Name")\
                                                         .withColumnRenamed("ScientificNotation_TotalCoinsMined", "TotalCoinsMined")\
                                                         .withColumnRenamed("Price", "Crypto_Price")

window_spec = Window.orderBy('Market_Cap')
df_dailyFact_serialNo = df_dailyFact_columnsRenamed.withColumn('SNo', row_number().over(window_spec))

df_dailyFact_Timestamp = df_dailyFact_serialNo.withColumn('CurrentTimestamp', current_timestamp())

df_dailyFact_final = df_dailyFact_Timestamp.select('SNo','Crypto','Crypto_Name','Currency','Market_Cap','TotalCoinsMined','Day','Month','Year')

path_dailyFact = '/content/drive/My Drive/mycolab_container/Fact/USD_DailyFact/'

df_dailyFact_final.write \
    .mode("overwrite") \
    .parquet(path_dailyFact)

