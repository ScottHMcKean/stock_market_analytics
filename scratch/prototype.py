# Databricks notebook source
# MAGIC %md
# MAGIC #Stock Market Data Loader
# MAGIC
# MAGIC This notebook prototypes functions for connection to Yahoo Finance and downloading ticker data - which we will use in DLT later

# COMMAND ----------

# MAGIC %pip install -r ../requirements-dev.txt

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd ../ 
# MAGIC pytest

# COMMAND ----------

# MAGIC %pip install yfinance pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
import yaml
from datetime import date

with open("watchlist.yml", "r") as file:
    watchlist = yaml.safe_load(file)

schema = T.StructType([
    T.StructField("ticker", T.StringType(), nullable=False),
    T.StructField("info", T.StringType(), nullable=True)
])

def fetch_ticker_info(tickers):
    result = []
    for ticker in tickers:
        info = yf.Ticker(ticker).get_info()
        info.pop('companyOfficers', None)
        result.append((ticker, info))
    return result

info_list = fetch_ticker_info(list(watchlist.keys()))
result_df = spark.createDataFrame(info_list, schema).withColumn('date', F.current_date())
display(result_df)

# COMMAND ----------

import yfinance as yf
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("info", MapType(StringType(), StringType()), True)
])

def fetch_ticker_info(tickers):
    result = []
    for ticker in tickers:
        info = yf.Ticker(ticker).get_info()
        info.pop('companyOfficers', None)
        result.append((ticker, info))
    return result

distinct_tickers = driver_table.select("ticker").distinct()
tickers_list = [row['ticker'] for row in distinct_tickers.collect()]
info_list = fetch_ticker_info(tickers_list)
result_df = spark.createDataFrame(info_list, schema).withColumn('date', F.current_date())
display(result_df)
