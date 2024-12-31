import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date
from stock_market_analytics.data import fetch_ticker_info, get_info_sdf

import os

def is_running_in_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

@pytest.fixture(scope="module")
def spark():
    if is_running_in_databricks():
        return spark
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_fetch_ticker_info(mocker):
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
    assert result_data[0]['info'] == "{key1=value1}"
    assert result_data[1]['info'] == "{key2=value2}"
    assert all(row['date'] == date.today() for row in result_data)
