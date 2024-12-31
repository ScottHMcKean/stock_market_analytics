import pyspark
import pyspark.sql.types as T
import pyspark.sql.functions as F
import yaml
from datetime import date
from typing import Dict
import yfinance as yf

def fetch_ticker_info(tickers: list):
    """
    Fetches information for a list of stock tickers.

    Args:
        tickers (list): A list of stock ticker symbols.

    Returns:
        list: A list of tuples, each containing a ticker symbol and its corresponding information dictionary.
    """
    result = []
    for ticker in tickers:
        info = yf.Ticker(ticker).get_info()
        info.pop('companyOfficers', None)
        result.append((ticker, info))
    return result

def get_info_sdf(watchlist: Dict, spark_session=None) -> pyspark.sql.DataFrame:
    """
    Retrieves stock information for a given watchlist and returns it as a Spark DataFrame.

    Args:
        watchlist (list): A list of stock ticker symbols.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the ticker symbol, its information, and the current date.
    """
    if not spark_session:
        spark_session = pyspark.sql.SparkSession.builder.getOrCreate()

    schema = T.StructType([
        T.StructField("ticker", T.StringType(), nullable=False),
        T.StructField("info", T.StringType(), nullable=True)
    ])

    info_list = fetch_ticker_info(list(watchlist.keys()))
    
    result_df = (spark_session
        .createDataFrame(info_list, schema)
        .withColumn('date', F.current_date())
        )
    
    return result_df