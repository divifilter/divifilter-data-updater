import unittest
from divifilter_data_updater.helper_functions import clean_numeric_value

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

if __name__ == '__main__':
    unittest.main()