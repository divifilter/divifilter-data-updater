import unittest
from unittest.mock import patch, MagicMock
from divifilter_data_updater.drip_investing_scraper import DripInvestingScraper


class TestDripInvestingScraper(unittest.TestCase):

    @patch('divifilter_data_updater.drip_investing_scraper.requests.Session')
    def test_get_tickers(self, mock_session):
        # Mock HTML response
        mock_html = '''
        <html>
            <a href="/stocks/jnj-dividend-history-calculator-returns/">JNJ</a>
            <a href="/stocks/pep-dividend-history-calculator-returns/">PEP</a>
            <a href="/stocks/ko-dividend-history-calculator-returns/">KO</a>
        </html>
        '''
        
        mock_response = MagicMock()
        mock_response.content = mock_html.encode('utf-8')
        mock_response.status_code = 200
        mock_session.return_value.get.return_value = mock_response
        
        scraper = DripInvestingScraper()
        tickers = scraper.get_tickers()
        
        self.assertEqual(len(tickers), 3)
        self.assertEqual(tickers[0]['symbol'], 'JNJ')
        self.assertTrue('dividend-history-calculator-returns' in tickers[0]['url'])

    @patch('divifilter_data_updater.drip_investing_scraper.requests.Session')
    def test_get_stock_data(self, mock_session):
        # Mock HTML response for a stock page
        mock_html = '''
        <html>
            <span class="years-tag">63 Years</span>
            <div class="data-row">
                <span class="data-label">Company</span>
                <span class="data-value">Johnson & Johnson</span>
            </div>
            <div class="data-row">
                <span class="data-label">Sector</span>
                <span class="data-value">Healthcare</span>
            </div>
            <div class="data-row">
                <span class="data-label">Price</span>
                <span class="data-value">$209.04</span>
            </div>
            <div class="data-row">
                <span class="data-label">Div Yield</span>
                <span class="data-value">2.49%</span>
            </div>
            <div class="data-row">
                <span class="data-label">DGR 5Y</span>
                <span class="data-value">5.2%</span>
            </div>
        </html>
        '''
        
        mock_response = MagicMock()
        mock_response.content = mock_html.encode('utf-8')
        mock_response.status_code = 200
        mock_session.return_value.get.return_value = mock_response
        
        scraper = DripInvestingScraper()
        stock_info = {'symbol': 'JNJ', 'url': 'https://www.dripinvesting.org/stocks/jnj-dividend-history-calculator-returns/'}
        data = scraper.get_stock_data(stock_info)
        
        self.assertIsNotNone(data)
        self.assertEqual(data['Symbol'], 'JNJ')
        self.assertEqual(data['Company'], 'Johnson & Johnson')
        self.assertEqual(data['Sector'], 'Healthcare')
        self.assertEqual(data['Price'], '$209.04')
        self.assertEqual(data['Div Yield'], '2.49%')
        self.assertEqual(data['DGR 5Y'], '5.2%')
        self.assertEqual(data['No Years'], '63')


if __name__ == '__main__':
    unittest.main()
