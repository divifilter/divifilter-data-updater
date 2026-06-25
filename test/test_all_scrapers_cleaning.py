import unittest
from unittest.mock import patch, MagicMock
from divifilter_data_updater.yahoo_finance import get_yahoo_finance_data_for_tickers_tuple
from divifilter_data_updater.drip_investing_scraper import DripInvestingScraper

class TestScrapersCleaning(unittest.TestCase):

    @patch('divifilter_data_updater.yahoo_finance.yf.Tickers')
    def test_yahoo_finance_cleaning(self, mock_tickers):
        mock_ticker = MagicMock()
        mock_ticker.info = {
            'currentPrice': "$150.00",
            'fiftyTwoWeekLow': "120,00",
            'fiftyTwoWeekHigh': "160.00%",
            'priceToBook': "5.5x",
            'payoutRatio': "0.65"
        }
        mock_tickers_instance = MagicMock()
        mock_tickers_instance.tickers = {'PG': mock_ticker}
        mock_tickers.return_value = mock_tickers_instance

        _, reply = get_yahoo_finance_data_for_tickers_tuple(("PG",))
        
        self.assertEqual(reply["PG"]["Price"], 150.0)
        self.assertEqual(reply["PG"]["Low"], 12000.0) # comma is removed, so 120,00 becomes 12000
        self.assertEqual(reply["PG"]["High"], 160.0)
        self.assertEqual(reply["PG"]["P/BV"], 5.5)
        self.assertEqual(reply["PG"]["Payout Ratio"], 65.0)

    @patch('divifilter_data_updater.drip_investing_scraper.requests.Session.get')
    def test_drip_investing_cleaning(self, mock_get):
        # Mocking the HTML response for a single stock
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = """
        <div class="data-row"><span class="data-label">Price</span><span class="data-value">$150.00</span></div>
        <div class="data-row"><span class="data-label">Div Yield</span><span class="data-value">4.50%</span></div>
        <span class="years-tag">63 Years</span>
        """
        mock_get.return_value = mock_response

        scraper = DripInvestingScraper()
        data = scraper.get_stock_data({"symbol": "PG", "url": "http://fakeurl.com"})
        
        self.assertEqual(data["Price"], 150.0)
        self.assertEqual(data["Div Yield"], 4.5)
        self.assertEqual(data["No Years"], 63.0)

if __name__ == '__main__':
    unittest.main()
