from stock_market_analytics.data import fetch_ticker_info, get_info_sdf

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("UnitTest").getOrCreate()

def test_fetch_ticker_info():
    with patch('yfinance.Ticker') as mock_ticker:
        mock_ticker.return_value.get_info.return_value = {'key1': 'value1', 'key2': 'value2', 'companyOfficers': 'should be removed'}
        result = fetch_ticker_info(['AAPL', 'GOOGL'])
        
        assert len(result) == 2
        assert result[0][0] == 'AAPL'
        assert result[1][0] == 'GOOGL'
        assert 'companyOfficers' not in result[0][1]
        assert 'companyOfficers' not in result[1][1]
        assert result[0][1] == {'key1': 'value1', 'key2': 'value2'}

def test_fetch_ticker_info_empty_list():
    result = fetch_ticker_info([])
    assert result == []

def test_fetch_ticker_info_error_handling():
    with patch('yfinance.Ticker') as mock_ticker:
        mock_ticker.return_value.get_info.side_effect = Exception("API Error")
        result = fetch_ticker_info(['AAPL'])
        assert result == [('AAPL', {})]

def test_get_info_sdf(spark):
    watchlist = {'AAPL': 'Apple Inc', 'GOOGL': 'Alphabet Inc'}
    
    with patch('your_module.fetch_ticker_info') as mock_fetch:
        mock_fetch.return_value = [
            ('AAPL', {'key1': 'value1'}),
            ('GOOGL', {'key2': 'value2'})
        ]
        
        result_df = get_info_sdf(watchlist, spark)
        
        expected_schema = StructType([
            StructField("ticker", StringType(), False),
            StructField("info", StringType(), True),
            StructField("date", DateType(), True)
        ])
        assert result_df.schema == expected_schema
        
        result_data = result_df.collect()
        assert len(result_data) == 2
        assert result_data[0]['ticker'] == 'AAPL'
        assert result_data[1]['ticker'] == 'GOOGL'
        assert result_data[0]['info'] == "{'key1': 'value1'}"
        assert result_data[1]['info'] == "{'key2': 'value2'}"
        assert result_data[0]['date'] == date.today()

def test_get_info_sdf_empty_watchlist(spark):
    watchlist = {}
    result_df = get_info_sdf(watchlist, spark)
    assert result_df.count() == 0

def test_get_info_sdf_no_spark_session():
    watchlist = {'AAPL': 'Apple Inc'}
    with patch('pyspark.sql.SparkSession.builder.getOrCreate') as mock_spark:
        mock_spark.return_value = MagicMock()
        get_info_sdf(watchlist)
        mock_spark.assert_called_once()
