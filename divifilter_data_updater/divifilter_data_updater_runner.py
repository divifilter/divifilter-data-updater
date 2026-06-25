import logging
import signal
import threading
import time

from divifilter_data_updater.drip_investing_scraper import DripInvestingScraper
from divifilter_data_updater.configure import read_configurations
from divifilter_data_updater.helper_functions import (
    remove_unneeded_columns,
    radar_dict_to_table,
    get_current_datetime_string,
    random_delay,
)
from divifilter_data_updater.db_functions import MysqlConnection
from divifilter_data_updater.yahoo_finance import (
    get_yahoo_finance_data_for_tickers_list,
    disable_yahoo_logs,
)

logger = logging.getLogger(__name__)

# Set when SIGTERM/SIGINT is received so the loop finishes the current cycle and
# exits cleanly instead of being SIGKILLed mid-update.
_stop_event = threading.Event()


def _request_shutdown(signum, _frame):
    logger.info("Received signal %s; will shut down after the current cycle", signum)
    _stop_event.set()


def _connect_with_retry(mysql_uri, max_attempts=5, base_delay=1, max_delay=30):
    """
    Acquire a MysqlConnection, retrying with bounded exponential backoff so a
    transient DB outage at connect time doesn't crash the whole process.

    Raises the last exception if every attempt fails.
    """
    attempt = 1
    while True:
        try:
            return MysqlConnection(mysql_uri)
        except Exception as e:
            if attempt >= max_attempts:
                raise
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            logger.warning("Database connection attempt %s/%s failed: %s; retrying in %ss",
                           attempt, max_attempts, e, delay)
            time.sleep(delay)
            attempt += 1


def init():
    # Register handlers so an orchestrator stop (SIGTERM) shuts us down cleanly.
    # Guarded because signals can only be registered from the main thread.
    try:
        signal.signal(signal.SIGTERM, _request_shutdown)
        signal.signal(signal.SIGINT, _request_shutdown)
    except ValueError:
        pass

    while not _stop_event.is_set():
        configuration = read_configurations()

        scraper = DripInvestingScraper(
            max_workers=configuration["scrape_max_workers"],
            stocks_url=configuration["dividend_radar_download_url"],
        )

        # disable yahoo spammy logs if set
        if configuration["disable_yahoo_logs"] is True:
            disable_yahoo_logs()

        try:
            database_connection = _connect_with_retry(configuration["mysql_uri"])
        except Exception as e:
            logger.error("Could not connect to the database, will retry next cycle: %s", e)
            random_delay(configuration["max_random_delay_seconds"], stop_event=_stop_event)
            continue

        with database_connection as mysql_connection:
            try:
                # Only re-scrape DripInvesting.org when it has published a new dataset
                # (its embedded updated_gmt advances). On unchanged days we skip the
                # ~800-page scrape entirely; Yahoo price enrichment below still runs.
                current_version = scraper.get_dataset_version()
                last_version = mysql_connection.check_db_update_dates().get("drip_updated_gmt")

                if current_version is not None and current_version == last_version:
                    logger.info("DripInvesting.org dataset unchanged (updated_gmt=%s); skipping scrape.",
                                current_version)
                else:
                    logger.info("Starting scrape from DripInvesting.org...")
                    # Scrape data
                    scraped_data_list = scraper.scrape_all_data()

                    if scraped_data_list:
                        # Convert list to dict format expected by helper functions (ticker -> data)
                        radar_dict = {item['Symbol']: item for item in scraped_data_list}

                        # Filter columns if needed (scraper already selects relevant data, but we can ensure cleanup)
                        unneeded_columns = ["FV", "None", None, "Ex-Date", "Pay-Date", "Website", "EPS 1Y"]

                        radar_dict_filtered = remove_unneeded_columns(radar_dict, unneeded_columns)

                        # Convert to dataframe and update DB
                        mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))

                        # Update metadata, including the dataset version we just scraped
                        mysql_connection.update_metadata_table({"radar_file": get_current_datetime_string()})
                        if current_version is not None:
                            mysql_connection.update_metadata_table({"drip_updated_gmt": current_version})
                        logger.info("Database updated successfully.")
                    else:
                        logger.warning("No data scraped.")

            except Exception as e:
                logger.exception("Error during update: %s", e)
                mysql_connection.conn.rollback()

            # always updated, just update all tickers and then update the timetable with yahoo & finviz update date to be later
            # also shown to enduser if it does not find that data in finviz fallback to yahoo and if not just keep what in the
            # db already
            if configuration["scrape_yahoo_finance"] is True or configuration["scrape_finviz"] is True:
                tickers_list = mysql_connection.get_tickers_from_db()

                if configuration["scrape_yahoo_finance"] is True:
                    mysql_connection.update_metadata_table({"yahoo_finance": get_current_datetime_string()})
                    yahoo_data = get_yahoo_finance_data_for_tickers_list(tickers_list)
                    mysql_connection.update_data_table(yahoo_data)

                # finviz library is currently broken, waiting for it to be fixed upstream
                # if configuration["scrape_finviz"] is True:
                #     mysql_connection.update_metadata_table({"finviz": get_current_datetime_string()})
                #     finviz_data = get_finviz_data_for_tickers_list(tickers_list)
                #     mysql_connection.update_data_table(finviz_data)

        # add a random delay between runs, if zero there will be none
        random_delay(configuration["max_random_delay_seconds"], stop_event=_stop_event)

    logger.info("Shutdown requested, exiting.")
