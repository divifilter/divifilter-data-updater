import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from divifilter_data_updater.yahoo_finance import *


class TestYahooFinance(unittest.TestCase):

    @patch('divifilter_data_updater.yahoo_finance.yf.Tickers')
    def test_get_yahoo_finance_data_for_tickers_tuple(self, mock_tickers):
        # Mock the yfinance response
        mock_ticker_pg = MagicMock()
        mock_ticker_pg.info = {
            'currentPrice': 150.0,
            'fiftyTwoWeekLow': 120.0,
            'fiftyTwoWeekHigh': 160.0,
            'priceToBook': 5.5,
            'payoutRatio': 0.65
        }
        
        mock_ticker_scl = MagicMock()
        mock_ticker_scl.info = {
            'currentPrice': 75.0,
            'fiftyTwoWeekLow': 60.0,
            'fiftyTwoWeekHigh': 80.0,
            'priceToBook': 3.2,
            'payoutRatio': 0.45
        }
        
        mock_tickers_instance = MagicMock()
        mock_tickers_instance.tickers = {
            'PG': mock_ticker_pg,
            'SCL': mock_ticker_scl
        }
        mock_tickers.return_value = mock_tickers_instance
        
        # Test with valid tickers
        time_reply, reply = get_yahoo_finance_data_for_tickers_tuple(("PG", "SCL"))
        
        self.assertIsInstance(time_reply, datetime)
        self.assertIsInstance(reply, dict)
        self.assertIn("PG", reply)
        self.assertIn("SCL", reply)
        self.assertEqual(reply["PG"]["Price"], 150.0)
        self.assertEqual(reply["SCL"]["Price"], 75.0)

    @patch('divifilter_data_updater.yahoo_finance.yf.Tickers')
    def test_get_yahoo_finance_data_for_tickers_list(self, mock_tickers):
        # Mock the yfinance response
        mock_ticker_pg = MagicMock()
        mock_ticker_pg.info = {
            'currentPrice': 150.0,
            'fiftyTwoWeekLow': 120.0,
            'fiftyTwoWeekHigh': 160.0,
            'priceToBook': 5.5,
            'payoutRatio': 0.65
        }
        
        mock_ticker_scl = MagicMock()
        mock_ticker_scl.info = {
            'currentPrice': 75.0,
            'fiftyTwoWeekLow': 60.0,
            'fiftyTwoWeekHigh': 80.0,
            'priceToBook': 3.2,
            'payoutRatio': 0.45
        }
        
        mock_tickers_instance = MagicMock()
        mock_tickers_instance.tickers = {
            'PG': mock_ticker_pg,
            'SCL': mock_ticker_scl
        }
        mock_tickers.return_value = mock_tickers_instance
        
        # Test with valid tickers
        time_reply, reply = get_yahoo_finance_data_for_tickers_list(["PG", "SCL"])
        
        self.assertIsInstance(time_reply, datetime)
        self.assertIsInstance(reply, dict)
        self.assertIn("PG", reply)
        self.assertIn("SCL", reply)
        self.assertEqual(reply["PG"]["Price"], 150.0)
        self.assertEqual(reply["SCL"]["Price"], 75.0)

    def test_disable_yahoo_logs(self):
        # Test that disable_yahoo_logs doesn't raise an error
        disable_yahoo_logs()
        logger = logging.getLogger('yfinance')
        self.assertTrue(logger.disabled)

