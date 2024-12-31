# Databricks notebook source
# MAGIC %md
# MAGIC #Stock Market Data Loader
# MAGIC
# MAGIC This notebook prototypes functions for connection to Yahoo Finance and downloading ticker data - which we will use in DLT later

# COMMAND ----------

# MAGIC %pip install -r ../requirements-dev.txt

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

# COMMAND ----------

import sys
sys.path.append('../src')

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date
from stock_market_analytics.data import fetch_ticker_info, get_info_sdf

# COMMAND ----------


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_fetch_ticker_info():
    # Mock yfinance.Ticker to return predictable data
    mocker().patch('yfinance.Ticker').return_value.get_info.return_value = {
        'key1': 'value1', 
        'key2': 'value2', 
        'companyOfficers': 'should be removed'
    }
    
    result = fetch_ticker_info(['AAPL', 'GOOGL'])
    
    assert len(result) == 2
    assert result[0] == ('AAPL', {'key1': 'value1', 'key2': 'value2'})
    assert result[1] == ('GOOGL', {'key1': 'value1', 'key2': 'value2'})
    assert all('companyOfficers' not in info for _, info in result)

test_fetch_ticker_info()


# COMMAND ----------

result = {
        'key1': 'value1', 
        'key2': 'value2', 
        'companyOfficers': 'should be removed'
    }

assert len(result) == 2
assert result[0] == ('AAPL', {'key1': 'value1', 'key2': 'value2'})
assert result[1] == ('GOOGL', {'key1': 'value1', 'key2': 'value2'})
assert all('companyOfficers' not in info for _, info in result)

# COMMAND ----------

# MAGIC %pip install pytest-mock

# COMMAND ----------

from pytest_mock import mocker
mock=mocker()

# COMMAND ----------

mocker.patch('')

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date
from stock_market_analytics.data import fetch_ticker_info, get_info_sdf

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_fetch_ticker_info():
    # Mock yfinance.Ticker to return predictable data
    mocker.patch('yfinance.Ticker').return_value.get_info.return_value = {
        'key1': 'value1', 
        'key2': 'value2', 
        'companyOfficers': 'should be removed'
    }
    
    result = fetch_ticker_info(['AAPL', 'GOOGL'])
    
    assert len(result) == 2
    assert result[0] == ('AAPL', {'key1': 'value1', 'key2': 'value2'})
    assert result[1] == ('GOOGL', {'key1': 'value1', 'key2': 'value2'})
    assert all('companyOfficers' not in info for _, info in result)

def test_get_info_sdf(spark, mocker):
    # Mock fetch_ticker_info to return predictable data
    mocker.patch('stock_market_analytics.data.fetch_ticker_info', return_value=[
        ('AAPL', {'key1': 'value1'}),
        ('GOOGL', {'key2': 'value2'})
    ])
    
    watchlist = {'AAPL': 'Apple Inc', 'GOOGL': 'Alphabet Inc'}
    result_df = get_info_sdf(watchlist, spark)
    
    # Check schema
    expected_schema = StructType([
        StructField("ticker", StringType(), False),
        StructField("info", StringType(), True),
        StructField("date", DateType(), False)
    ])
    assert result_df.schema == expected_schema
    
    # Check data
    result_data = result_df.collect()
    assert len(result_data) == 2
    assert result_data[0]['ticker'] == 'AAPL'
    assert result_data[1]['ticker'] == 'GOOGL'
    assert result_data[0]['info'] == {'key1': 'value1'}
    assert result_data[1]['info'] == {'key2': 'value2'}
    assert all(row['date'] == date.today() for row in result_data)

