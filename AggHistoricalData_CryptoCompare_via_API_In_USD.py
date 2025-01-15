from google.colab import drive
drive.mount('/content/drive')
# drive.flush_and_unmount()

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Append Parquet") \
    .getOrCreate()

from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
class DataFrameConverter:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_pandas(self, df):
        """
        Converts the given DataFrame to Pandas if it's a Spark DataFrame.
        Leaves it as a Pandas DataFrame if it's already a Pandas DataFrame.
        
        :param df: The input DataFrame (Spark or Pandas).
        :return: A Pandas DataFrame.
        """
        if isinstance(df, pd.DataFrame):
            # If it's already a Pandas DataFrame, return as is
            print("DataFrame is already a Pandas DataFrame.")
            return df
        elif isinstance(df, SparkDataFrame):
            # If it's a Spark DataFrame, convert it to Pandas
            print("Done converting Spark DataFrame to Pandas DataFrame.")
            return df.toPandas()
        else:
            raise TypeError("Input must be either a Pandas DataFrame or a Spark DataFrame.")

class_converter = DataFrameConverter(spark)

# Importing required pyspark modules
from pyspark.sql.functions import col, row_number, current_timestamp, lit
from pyspark.sql.window import Window

now = datetime.now()
current_month_name = now.strftime('%B')
current_year = now.year
current_year_str = str(current_year)
curr_month_year = current_month_name + current_year_str
day = now.day
current_month = current_month_name[:3]
current_month = now.month
two_digit_month = f"{current_month:02d}"  # Format as two-digit string
current_month_int = int(current_month)  # Convert to integer

path_historical_data = '/content/drive/My Drive/mycolab_container/Fact/USD_HistoricalData/'

df_historical_crypto_data = spark.read.parquet(path_historical_data)

df_historical_withDate = df_historical_crypto_data.withColumn("Date_column",to_date("Timestamp"))

df_sorted_date = df_historical_withDate.orderBy("Date_column")

window_spec_date_asc = Window.partitionBy("Crypto","Currency").orderBy("Date_column")

df_with_change = df_sorted_date.withColumn("Price_Change_24Hours", F.col("Price") - F.lag("Price").over(window_spec_date_asc))\
                                                .withColumn("PriceChange_7Days", F.col("Price") - F.lag("Price",7).over(window_spec_date_asc))\
                                                .withColumn("PriceChange_15Days", F.col("Price") - F.lag("Price", 15).over(window_spec_date_asc))\
                                                .withColumn("PriceChange_1Month",F.col("Price") - F.lag("Price", 30).over(window_spec_date_asc))\
                                                .withColumn("PriceChange_3Months",F.col("Price") - F.lag("Price", 90).over(window_spec_date_asc))\
                                                .withColumn("PriceChange_6Months",F.col("Price") - F.lag("Price", 180).over(window_spec_date_asc))\
                                                            .withColumn("PriceChange_1Year",F.col("Price") - F.lag("Price", 365).over(window_spec_date_asc))

# Dashboarding level Information
change_datapath = '/content/drive/My Drive/mycolab_container/FinalData/USD_Change_HistoricalData/'

df_with_change.write \
    .mode("append") \
    .parquet(change_datapath)
