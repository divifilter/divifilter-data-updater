import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd
from divifilter_data_updater.helper_functions import (
    clean_numeric_value, radar_dict_to_table, remove_unneeded_columns,
    get_current_datetime_string, random_delay
)


class TestHelperFunctions(unittest.TestCase):
    def test_clean_numeric_value_basic(self):
        self.assertEqual(clean_numeric_value("123.45"), 123.45)
        self.assertEqual(clean_numeric_value(123.45), 123.45)
        self.assertEqual(clean_numeric_value("1,234.56"), 1234.56)

    def test_clean_numeric_value_currency(self):
        self.assertEqual(clean_numeric_value("$123.45"), 123.45)
        self.assertEqual(clean_numeric_value("-$123.45"), -123.45)

    def test_clean_numeric_value_percentage(self):
        self.assertEqual(clean_numeric_value("5.5%"), 5.5)
        self.assertEqual(clean_numeric_value("0.65%"), 0.65)

    def test_clean_numeric_value_abbreviations(self):
        self.assertEqual(clean_numeric_value("1.5M"), 1.5)
        self.assertEqual(clean_numeric_value("2.3B"), 2.3)
        self.assertEqual(clean_numeric_value("15x"), 15.0)

    def test_clean_numeric_value_empty_none(self):
        self.assertIsNone(clean_numeric_value(""))
        self.assertIsNone(clean_numeric_value(None))
        self.assertIsNone(clean_numeric_value("N/A"))
        self.assertIsNone(clean_numeric_value("-"))
        self.assertIsNone(clean_numeric_value("None"))

    def test_clean_numeric_value_invalid(self):
        self.assertIsNone(clean_numeric_value("abc"))
        self.assertIsNone(clean_numeric_value("1.2.3"))

class TestRadarDictToTable(unittest.TestCase):

    def test_multi_ticker_dict(self):
        data = {
            "AAPL": {"Price": 150, "Yield": 1.5},
            "MSFT": {"Price": 300, "Yield": 0.8}
        }
        df = radar_dict_to_table(data)
        self.assertEqual(len(df), 2)
        self.assertIn("Price", df.columns)
        self.assertIn("Yield", df.columns)

    def test_empty_dict(self):
        df = radar_dict_to_table({})
        self.assertTrue(df.empty)

    def test_single_entry(self):
        data = {"AAPL": {"Price": 150, "Yield": 1.5}}
        df = radar_dict_to_table(data)
        self.assertEqual(df.shape[0], 1)


class TestRemoveUnneededColumns(unittest.TestCase):

    def test_removes_specified_keys(self):
        data = {
            "AAPL": {"Price": 150, "FV": 10, "Yield": 1.5},
            "MSFT": {"Price": 300, "FV": 20, "Yield": 0.8}
        }
        result = remove_unneeded_columns(data, ["FV"])
        self.assertNotIn("FV", result["AAPL"])
        self.assertNotIn("FV", result["MSFT"])
        self.assertIn("Price", result["AAPL"])

    def test_handles_missing_keys(self):
        data = {"AAPL": {"Price": 150}}
        result = remove_unneeded_columns(data, ["NonExistent"])
        self.assertEqual(result, {"AAPL": {"Price": 150}})

    def test_removes_none_key(self):
        data = {"AAPL": {None: "junk", "Price": 150}}
        result = remove_unneeded_columns(data, [None])
        self.assertNotIn(None, result["AAPL"])
        self.assertIn("Price", result["AAPL"])

    def test_empty_dict(self):
        result = remove_unneeded_columns({}, ["FV"])
        self.assertEqual(result, {})


class TestGetCurrentDatetimeString(unittest.TestCase):

    def test_format(self):
        result = get_current_datetime_string()
        # Verify the format matches YYYY-MM-DD HH:MM:SS
        datetime.strptime(result, "%Y-%m-%d %H:%M:%S")


class TestRandomDelay(unittest.TestCase):

    @patch('divifilter_data_updater.helper_functions.time.sleep')
    @patch('divifilter_data_updater.helper_functions.random.randint', return_value=5)
    def test_calls_sleep(self, mock_randint, mock_sleep):
        random_delay(100)
        mock_randint.assert_called_once_with(0, 100)
        mock_sleep.assert_called_once_with(5)

    @patch('divifilter_data_updater.helper_functions.time.sleep')
    @patch('divifilter_data_updater.helper_functions.random.randint', return_value=0)
    def test_zero_max(self, mock_randint, mock_sleep):
        random_delay(0)
        mock_randint.assert_called_once_with(0, 0)
        mock_sleep.assert_called_once_with(0)


if __name__ == '__main__':
    unittest.main()