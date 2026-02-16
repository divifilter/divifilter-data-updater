import unittest
from unittest.mock import patch, MagicMock, mock_open
from divifilter_data_updater.dividend_radar import DividendRadar


class TestDividendRadarInit(unittest.TestCase):

    def test_init_sets_attributes(self):
        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        self.assertEqual(radar.dividend_radar_url, "http://example.com")
        self.assertEqual(radar.local_file, "/tmp/file.xlsx")
        self.assertIsNone(radar.latest_version_url)
        self.assertIsNone(radar.latest_version)
        self.assertIsNone(radar.latest_local_version)


class TestFindLatestVersion(unittest.TestCase):

    @patch('divifilter_data_updater.dividend_radar.requests.get')
    def test_success(self, mock_get):
        html = '''
        <html>
            <a class="link-block w-inline-block"
               href="https://example.com/files/DR_2026-02-14.xlsx">Download</a>
        </html>
        '''
        mock_response = MagicMock()
        mock_response.content = html.encode('utf-8')
        mock_get.return_value = mock_response

        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        result = radar.find_latest_version()

        self.assertEqual(result, "2026-02-14")
        self.assertEqual(radar.latest_version, "2026-02-14")
        self.assertIn("DR_2026-02-14.xlsx", radar.latest_version_url)

    @patch('divifilter_data_updater.dividend_radar.requests.get')
    def test_no_link_raises(self, mock_get):
        html = '<html><body>No links here</body></html>'
        mock_response = MagicMock()
        mock_response.content = html.encode('utf-8')
        mock_get.return_value = mock_response

        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        # Unwrap the retry decorator to test the inner function directly
        inner_fn = radar.find_latest_version.__wrapped__
        with self.assertRaises(AttributeError):
            inner_fn(radar)


class TestCheckIfLocalIsLatest(unittest.TestCase):

    def test_matching_version_returns_true(self):
        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        radar.latest_local_version = "2026-02-14"
        with patch.object(radar, 'find_latest_version', return_value="2026-02-14"):
            self.assertTrue(radar.check_if_local_is_latest())

    def test_different_version_returns_false(self):
        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        radar.latest_local_version = "2026-02-10"
        with patch.object(radar, 'find_latest_version', return_value="2026-02-14"):
            self.assertFalse(radar.check_if_local_is_latest())

    def test_file_not_found_returns_false(self):
        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        with patch.object(radar, 'find_latest_version', side_effect=FileNotFoundError):
            self.assertFalse(radar.check_if_local_is_latest())


class TestDownloadLatestVersion(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open)
    @patch('divifilter_data_updater.dividend_radar.requests.get')
    def test_downloads_and_writes_file(self, mock_get, mock_file):
        # First call: find_latest_version page
        page_html = '''
        <html>
            <a class="link-block w-inline-block"
               href="https://example.com/files/DR_2026-02-14.xlsx">Download</a>
        </html>
        '''
        mock_page = MagicMock()
        mock_page.content = page_html.encode('utf-8')

        mock_download = MagicMock()
        mock_download.content = b"fake xlsx bytes"

        mock_get.side_effect = [mock_page, mock_download]

        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        radar.download_latest_version()

        mock_file.assert_called_once_with("/tmp/file.xlsx", 'wb')
        mock_file().write.assert_called_once_with(b"fake xlsx bytes")
        self.assertEqual(radar.latest_local_version, "2026-02-14")


class TestReadRadarFileToDict(unittest.TestCase):

    @patch('divifilter_data_updater.dividend_radar.openpyxl.load_workbook')
    def test_parses_workbook(self, mock_load):
        # Set up mock workbook with header row (row 3) and data rows (row 4+)
        mock_wb = MagicMock()
        mock_sheet = MagicMock()
        mock_wb.__getitem__ = MagicMock(return_value=mock_sheet)

        # Data rows
        cell_aapl_sym = MagicMock(value="AAPL", column=1)
        cell_aapl_price = MagicMock(value=150.0, column=2)
        cell_msft_sym = MagicMock(value="MSFT", column=1)
        cell_msft_price = MagicMock(value=300.0, column=2)

        mock_sheet.iter_rows.return_value = [
            [cell_aapl_sym, cell_aapl_price],
            [cell_msft_sym, cell_msft_price],
        ]

        # Header row (row 3)
        header_sym = MagicMock(value="Symbol")
        header_price = MagicMock(value="Price")
        mock_sheet.cell.side_effect = lambda row, column: {
            (3, 1): header_sym, (3, 2): header_price
        }[(row, column)]

        mock_load.return_value = mock_wb

        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        result = radar.read_radar_file_to_dict()

        self.assertIn("AAPL", result)
        self.assertIn("MSFT", result)
        self.assertEqual(result["AAPL"]["Price"], 150.0)
        self.assertEqual(result["MSFT"]["Price"], 300.0)

    @patch('divifilter_data_updater.dividend_radar.openpyxl.load_workbook')
    def test_empty_sheet(self, mock_load):
        mock_wb = MagicMock()
        mock_sheet = MagicMock()
        mock_wb.__getitem__ = MagicMock(return_value=mock_sheet)
        mock_sheet.iter_rows.return_value = []
        mock_load.return_value = mock_wb

        radar = DividendRadar("http://example.com", "/tmp/file.xlsx")
        result = radar.read_radar_file_to_dict()
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()
