import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import math
from divifilter_data_updater.db_functions import MysqlConnection


@patch('divifilter_data_updater.db_functions.create_engine')
class TestMysqlConnectionInit(unittest.TestCase):

    def test_init_with_valid_mysql_uri(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://user:pass@host/db",
            connect_args={},
            echo=False
        )
        self.assertIsNotNone(conn.engine)
        self.assertIsNotNone(conn.conn)
        self.assertIsNotNone(conn.meta)

    def test_init_with_pymysql_uri_no_double_replace(self, mock_create_engine):
        MysqlConnection("mysql+pymysql://user:pass@host/db")
        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://user:pass@host/db",
            connect_args={},
            echo=False
        )

    def test_init_with_empty_uri_raises_value_error(self, mock_create_engine):
        with self.assertRaises(ValueError):
            MysqlConnection("")

    def test_init_with_none_uri_raises_value_error(self, mock_create_engine):
        with self.assertRaises(ValueError):
            MysqlConnection(None)

    def test_init_with_aiven_uri_sets_ssl(self, mock_create_engine):
        MysqlConnection("mysql://user:pass@host.aivencloud.com/db")
        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://user:pass@host.aivencloud.com/db",
            connect_args={'ssl': {'ssl_mode': 'REQUIRED'}},
            echo=False
        )

    def test_init_without_aiven_uri_no_ssl(self, mock_create_engine):
        MysqlConnection("mysql://user:pass@host/db")
        mock_create_engine.assert_called_once_with(
            "mysql+pymysql://user:pass@host/db",
            connect_args={},
            echo=False
        )


@patch('divifilter_data_updater.db_functions.create_engine')
class TestUpdateDataTableFromDataFrame(unittest.TestCase):

    def test_calls_to_sql_with_correct_args(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        df = pd.DataFrame({"Symbol": ["AAPL"], "Price": [150.0]})
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            conn.update_data_table_from_data_frame(df)
            mock_to_sql.assert_called_once()
            args, kwargs = mock_to_sql.call_args
            self.assertEqual(args[0], "dividend_data_table")
            self.assertEqual(kwargs['if_exists'], "replace")
            self.assertEqual(kwargs['index'], False)
            self.assertIn('Symbol', kwargs['dtype'])

    def test_dtype_map_has_all_expected_keys(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        df = pd.DataFrame()
        expected_keys = {
            'Symbol', 'No Years', 'Price', 'Div Yield', '5Y Avg Yield',
            'Current Div', 'Annualized', 'Low', 'High', 'DGR 1Y', 'DGR 3Y',
            'DGR 5Y', 'DGR 10Y', 'TTR 1Y', 'TTR 3Y', 'FV %', 'Fair Value',
            'Chowder Number', 'Revenue 1Y', 'NPM', 'CF/Share', 'ROE',
            'Debt/Capital', 'ROTC', 'P/E', 'P/BV', 'PEG', 'Market Cap'
        }
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            conn.update_data_table_from_data_frame(df)
            dtype = mock_to_sql.call_args[1]['dtype']
            self.assertEqual(set(dtype.keys()), expected_keys)

    def test_empty_dataframe_no_crash(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        df = pd.DataFrame()
        with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
            conn.update_data_table_from_data_frame(df)
            mock_to_sql.assert_called_once()


@patch('divifilter_data_updater.db_functions.create_engine')
class TestUpdateMetadataTable(unittest.TestCase):

    def test_single_entry(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.update_metadata_table({"radar_file": "2026-02-16 12:00:00"})
        conn.conn.execute.assert_called_once()
        conn.conn.commit.assert_called_once()

    def test_multiple_entries(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.update_metadata_table({"a": "1", "b": "2", "c": "3"})
        self.assertEqual(conn.conn.execute.call_count, 3)
        self.assertEqual(conn.conn.commit.call_count, 3)

    def test_parameterized_query(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.update_metadata_table({"radar_file": "2026-02-16"})
        args = conn.conn.execute.call_args[0]
        params = conn.conn.execute.call_args
        # Second argument should be the params dict
        self.assertEqual(params[0][1], {"name": "radar_file", "update_time": "2026-02-16"})


@patch('divifilter_data_updater.db_functions.create_engine')
class TestRunSqlQuery(unittest.TestCase):

    def test_select_returns_fetchall(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("AAPL",), ("MSFT",)]
        conn.conn.execute.return_value = mock_result

        result = conn.run_sql_query("SELECT * FROM table")
        self.assertEqual(result, [("AAPL",), ("MSFT",)])
        conn.conn.commit.assert_not_called()

    def test_update_returns_none_and_commits(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        result = conn.run_sql_query("UPDATE table SET col = 1")
        self.assertIsNone(result)
        conn.conn.commit.assert_called_once()

    def test_insert_returns_none_and_commits(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        result = conn.run_sql_query("INSERT INTO table VALUES (1)")
        self.assertIsNone(result)
        conn.conn.commit.assert_called_once()


@patch('divifilter_data_updater.db_functions.create_engine')
class TestCheckDbUpdateDates(unittest.TestCase):

    def test_table_exists(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("radar_file", "2026-01-01")]
        conn.conn.execute.return_value = mock_result

        result = conn.check_db_update_dates()
        self.assertEqual(result, {"radar_file": "2026-01-01"})

    def test_table_not_exists_creates_table(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = False
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        conn.conn.execute.return_value = mock_result

        with patch.object(conn.dividend_update_times, 'create') as mock_create:
            result = conn.check_db_update_dates()
            mock_create.assert_called_once_with(conn.conn)
        self.assertEqual(result, {})


@patch('divifilter_data_updater.db_functions.inspect')
@patch('divifilter_data_updater.db_functions.create_engine')
class TestUpdateDataTable(unittest.TestCase):

    def test_valid_numeric_data(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = [{'name': 'Price'}]

        from datetime import datetime
        data = (datetime.now(), {"AAPL": {"Price": 150.0}})
        conn.update_data_table(data)

        self.assertTrue(conn.conn.execute.called)
        self.assertTrue(conn.conn.commit.called)

    def test_creates_missing_columns(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = []

        from datetime import datetime
        data = (datetime.now(), {"AAPL": {"NewCol": 42.0}})

        with patch.object(conn, 'run_sql_query') as mock_run:
            conn.update_data_table(data)
            alter_calls = [c for c in mock_run.call_args_list
                           if 'ALTER TABLE' in str(c)]
            self.assertTrue(len(alter_calls) > 0)

    def test_creates_table_if_not_exists(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = False

        from datetime import datetime
        data = (datetime.now(), {})
        with patch.object(conn, 'create_dividend_data_table') as mock_create:
            conn.update_data_table(data)
            mock_create.assert_called_once()

    def test_invalid_tuple_raises_error(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True

        with self.assertRaises(ValueError):
            conn.update_data_table("not a tuple")

        with self.assertRaises(ValueError):
            conn.update_data_table(("only_one_element",))

    def test_skips_none_values(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = [{'name': 'Price'}]

        from datetime import datetime
        data = (datetime.now(), {"AAPL": {"Price": None}})
        conn.conn.execute.reset_mock()
        conn.update_data_table(data)
        conn.conn.execute.assert_not_called()

    def test_skips_nan_values(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = [{'name': 'Price'}]

        from datetime import datetime
        data = (datetime.now(), {"AAPL": {"Price": float('nan')}})
        conn.conn.execute.reset_mock()
        conn.update_data_table(data)
        conn.conn.execute.assert_not_called()

    def test_handles_string_values(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        conn.engine.dialect.has_table.return_value = True
        mock_inspect.return_value.get_columns.return_value = [{'name': 'Company'}]

        from datetime import datetime
        data = (datetime.now(), {"AAPL": {"Company": "Apple Inc"}})
        conn.update_data_table(data)

        self.assertTrue(conn.conn.execute.called)
        self.assertTrue(conn.conn.commit.called)


@patch('divifilter_data_updater.db_functions.Table')
@patch('divifilter_data_updater.db_functions.inspect')
@patch('divifilter_data_updater.db_functions.create_engine')
class TestCreateDividendDataTable(unittest.TestCase):

    def test_creates_table_when_not_exists(self, mock_create_engine, mock_inspect, mock_table):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.has_table.return_value = False
        conn.create_dividend_data_table()
        mock_inspect.return_value.has_table.assert_called_with("dividend_data_table")
        mock_table.return_value.create.assert_called_once_with(conn.conn)

    def test_skips_creation_when_exists(self, mock_create_engine, mock_inspect, mock_table):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.has_table.return_value = True
        conn.create_dividend_data_table()
        mock_inspect.return_value.has_table.assert_called_with("dividend_data_table")
        mock_table.return_value.create.assert_not_called()


@patch('divifilter_data_updater.db_functions.inspect')
@patch('divifilter_data_updater.db_functions.create_engine')
class TestGetTickersFromDb(unittest.TestCase):

    def test_table_exists_returns_tickers(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.has_table.return_value = True

        mock_result = MagicMock()
        mock_result.fetchall.return_value = [("AAPL",), ("MSFT",), ("JNJ",)]
        conn.conn.execute.return_value = mock_result

        tickers = conn.get_tickers_from_db()
        self.assertEqual(tickers, ["AAPL", "MSFT", "JNJ"])

    def test_table_not_exists_returns_empty_list(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.has_table.return_value = False

        tickers = conn.get_tickers_from_db()
        self.assertEqual(tickers, [])

    def test_empty_table_returns_empty_list(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.has_table.return_value = True

        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        conn.conn.execute.return_value = mock_result

        tickers = conn.get_tickers_from_db()
        self.assertEqual(tickers, [])


@patch('divifilter_data_updater.db_functions.inspect')
@patch('divifilter_data_updater.db_functions.create_engine')
class TestColumnExists(unittest.TestCase):

    def test_column_exists_true(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.get_columns.return_value = [
            {'name': 'Symbol'}, {'name': 'Price'}
        ]
        self.assertTrue(conn.column_exists("dividend_data_table", "Symbol"))

    def test_column_exists_false(self, mock_create_engine, mock_inspect):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_inspect.return_value.get_columns.return_value = [
            {'name': 'Symbol'}, {'name': 'Price'}
        ]
        self.assertFalse(conn.column_exists("dividend_data_table", "NonExistent"))


@patch('divifilter_data_updater.db_functions.create_engine')
class TestAddColumnToTable(unittest.TestCase):

    def test_add_column(self, mock_create_engine):
        conn = MysqlConnection("mysql://user:pass@host/db")
        mock_result = MagicMock()
        conn.conn.execute.return_value = mock_result

        conn.add_column_to_table("dividend_data_table", "New Column")
        conn.conn.execute.assert_called()
        conn.conn.commit.assert_called()


if __name__ == '__main__':
    unittest.main()
