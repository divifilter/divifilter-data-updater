import unittest
from unittest.mock import patch, MagicMock


class BreakLoop(Exception):
    """Custom exception to break the infinite loop in init()."""
    pass


def _default_config(**overrides):
    config = {
        "mysql_uri": "mysql://user:pass@host/db",
        "dividend_radar_download_url": "https://www.dripinvesting.org/stocks/",
        "scrape_yahoo_finance": False,
        "scrape_finviz": False,
        "disable_yahoo_logs": False,
        "max_random_delay_seconds": 0,
        "scrape_max_workers": 4,
    }
    config.update(overrides)
    return config


@patch('divifilter_data_updater.divifilter_data_updater_runner.random_delay', side_effect=BreakLoop)
@patch('divifilter_data_updater.divifilter_data_updater_runner.get_current_datetime_string', return_value="2026-02-16 12:00:00")
@patch('divifilter_data_updater.divifilter_data_updater_runner.MysqlConnection')
@patch('divifilter_data_updater.divifilter_data_updater_runner.DripInvestingScraper')
@patch('divifilter_data_updater.divifilter_data_updater_runner.read_configurations')
class TestInit(unittest.TestCase):

    def test_full_flow_scrape_and_yahoo(self, mock_config, mock_scraper_cls,
                                        mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config(
            scrape_yahoo_finance=True, disable_yahoo_logs=True
        )
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.return_value = [
            {"Symbol": "AAPL", "Price": 150.0},
        ]
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with patch('divifilter_data_updater.divifilter_data_updater_runner.disable_yahoo_logs') as mock_disable, \
             patch('divifilter_data_updater.divifilter_data_updater_runner.get_yahoo_finance_data_for_tickers_list') as mock_yahoo:
            with self.assertRaises(BreakLoop):
                from divifilter_data_updater.divifilter_data_updater_runner import init
                init()

            # Scraper was called
            scraper.scrape_all_data.assert_called_once()
            # DB was updated
            mysql.update_data_table_from_data_frame.assert_called_once()
            mysql.update_metadata_table.assert_called()
            # Yahoo was called
            mock_yahoo.assert_called_once()
            mysql.update_data_table.assert_called_once()
            # Yahoo logs disabled
            mock_disable.assert_called_once()

    def test_no_data_scraped(self, mock_config, mock_scraper_cls,
                              mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config()
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.return_value = []
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with self.assertRaises(BreakLoop):
            from divifilter_data_updater.divifilter_data_updater_runner import init
            init()

        mysql.update_data_table_from_data_frame.assert_not_called()

    def test_scraper_exception_handled(self, mock_config, mock_scraper_cls,
                                        mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config()
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.side_effect = Exception("network error")
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with self.assertRaises(BreakLoop):
            from divifilter_data_updater.divifilter_data_updater_runner import init
            init()

        # Exception is caught, DB write not called
        mysql.update_data_table_from_data_frame.assert_not_called()

    def test_scraper_exception_rolls_back_connection(self, mock_config, mock_scraper_cls,
                                                      mock_mysql_cls, mock_datetime, mock_delay):
        """After a scrape error, rollback so Yahoo scraping can still use the connection."""
        mock_config.return_value = _default_config(scrape_yahoo_finance=True)
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.side_effect = Exception("network error")
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with patch('divifilter_data_updater.divifilter_data_updater_runner.get_yahoo_finance_data_for_tickers_list'):
            with self.assertRaises(BreakLoop):
                from divifilter_data_updater.divifilter_data_updater_runner import init
                init()

        mysql.conn.rollback.assert_called_once()
        mysql.get_tickers_from_db.assert_called_once()

    def test_yahoo_disabled(self, mock_config, mock_scraper_cls,
                             mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config(
            scrape_yahoo_finance=False, scrape_finviz=False
        )
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.return_value = []
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with patch('divifilter_data_updater.divifilter_data_updater_runner.get_yahoo_finance_data_for_tickers_list') as mock_yahoo:
            with self.assertRaises(BreakLoop):
                from divifilter_data_updater.divifilter_data_updater_runner import init
                init()
            mock_yahoo.assert_not_called()
            mysql.get_tickers_from_db.assert_not_called()

    def test_disable_yahoo_logs_called(self, mock_config, mock_scraper_cls,
                                        mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config(disable_yahoo_logs=True)
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.return_value = []
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with patch('divifilter_data_updater.divifilter_data_updater_runner.disable_yahoo_logs') as mock_disable:
            with self.assertRaises(BreakLoop):
                from divifilter_data_updater.divifilter_data_updater_runner import init
                init()
            mock_disable.assert_called_once()

    def test_disable_yahoo_logs_not_called_when_false(self, mock_config, mock_scraper_cls,
                                                       mock_mysql_cls, mock_datetime, mock_delay):
        mock_config.return_value = _default_config(disable_yahoo_logs=False)
        scraper = mock_scraper_cls.return_value
        scraper.scrape_all_data.return_value = []
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)

        with patch('divifilter_data_updater.divifilter_data_updater_runner.disable_yahoo_logs') as mock_disable:
            with self.assertRaises(BreakLoop):
                from divifilter_data_updater.divifilter_data_updater_runner import init
                init()
            mock_disable.assert_not_called()

    def test_skips_scrape_when_version_unchanged(self, mock_config, mock_scraper_cls,
                                                 mock_mysql_cls, mock_datetime, mock_delay):
        """When updated_gmt matches the stored version, the scrape is skipped entirely."""
        mock_config.return_value = _default_config()
        scraper = mock_scraper_cls.return_value
        scraper.get_dataset_version.return_value = "2026-06-24 03:09:53"
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)
        mysql.check_db_update_dates.return_value = {"drip_updated_gmt": "2026-06-24 03:09:53"}

        with self.assertRaises(BreakLoop):
            from divifilter_data_updater.divifilter_data_updater_runner import init
            init()

        scraper.scrape_all_data.assert_not_called()
        mysql.update_data_table_from_data_frame.assert_not_called()

    def test_scrapes_when_version_changed(self, mock_config, mock_scraper_cls,
                                          mock_mysql_cls, mock_datetime, mock_delay):
        """A new updated_gmt triggers a full scrape and stores the new version."""
        mock_config.return_value = _default_config()
        scraper = mock_scraper_cls.return_value
        scraper.get_dataset_version.return_value = "2026-06-25 03:09:53"
        scraper.scrape_all_data.return_value = [{"Symbol": "AAPL", "Price": 150.0}]
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)
        mysql.check_db_update_dates.return_value = {"drip_updated_gmt": "2026-06-24 03:09:53"}

        with self.assertRaises(BreakLoop):
            from divifilter_data_updater.divifilter_data_updater_runner import init
            init()

        scraper.scrape_all_data.assert_called_once()
        mysql.update_data_table_from_data_frame.assert_called_once()
        mysql.update_metadata_table.assert_any_call({"drip_updated_gmt": "2026-06-25 03:09:53"})

    def test_scrapes_when_version_unavailable(self, mock_config, mock_scraper_cls,
                                              mock_mysql_cls, mock_datetime, mock_delay):
        """Fail safe: if updated_gmt can't be read (None), do a full scrape rather than skip."""
        mock_config.return_value = _default_config()
        scraper = mock_scraper_cls.return_value
        scraper.get_dataset_version.return_value = None
        scraper.scrape_all_data.return_value = [{"Symbol": "AAPL", "Price": 150.0}]
        mysql = mock_mysql_cls.return_value
        mysql.__enter__ = MagicMock(return_value=mysql)
        mysql.__exit__ = MagicMock(return_value=False)
        mysql.check_db_update_dates.return_value = {"drip_updated_gmt": "2026-06-24 03:09:53"}

        with self.assertRaises(BreakLoop):
            from divifilter_data_updater.divifilter_data_updater_runner import init
            init()

        scraper.scrape_all_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
