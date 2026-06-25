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
    validate_radar_data,
)
from divifilter_data_updater.db_functions import MysqlConnection
from divifilter_data_updater.yahoo_finance import (
    get_yahoo_finance_data_for_tickers_list,
    disable_yahoo_logs,
)
from divifilter_data_updater.health import write_heartbeat

logger = logging.getLogger(__name__)

# Set when SIGTERM/SIGINT is received so the loop finishes the current cycle and
# exits cleanly instead of being SIGKILLed mid-update.
_stop_event = threading.Event()


def _request_shutdown(signum, _frame):
    logger.info("Received signal %s; will shut down after the current cycle", signum)
    _stop_event.set()


def _fmt_age(seconds):
    """Render an age-in-seconds value for logging, tolerating None/non-numeric."""
    if not isinstance(seconds, (int, float)):
        return "unknown"
    return f"{seconds:.0f}s ago"


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

        # Liveness heartbeat for the Docker healthcheck (detects a hung loop).
        write_heartbeat(configuration["max_random_delay_seconds"])

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
                # Surface data freshness so staleness is visible in the logs.
                logger.info("Data freshness — radar: %s, yahoo: %s",
                            _fmt_age(mysql_connection.get_update_age_seconds("radar_file")),
                            _fmt_age(mysql_connection.get_update_age_seconds("yahoo_finance")))

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
                    scraped_count = len(scraped_data_list)
                    min_expected = configuration["scrape_min_expected_tickers"]

                    if scraped_count >= min_expected:
                        # Convert list to dict format expected by helper functions (ticker -> data)
                        radar_dict = {item['Symbol']: item for item in scraped_data_list}

                        # Filter columns if needed (scraper already selects relevant data, but we can ensure cleanup)
                        unneeded_columns = ["FV", "None", None, "Ex-Date", "Pay-Date", "Website", "EPS 1Y"]

                        radar_dict_filtered = remove_unneeded_columns(radar_dict, unneeded_columns)

                        # Null any obviously-bad values before they reach the DB
                        validate_radar_data(radar_dict_filtered)

                        # Convert to dataframe and update DB
                        mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))

                        # Update metadata, including the dataset version we just scraped
                        mysql_connection.update_metadata_table({"radar_file": get_current_datetime_string()})
                        if current_version is not None:
                            mysql_connection.update_metadata_table({"drip_updated_gmt": current_version})
                        logger.info("Database updated successfully (%s stocks).", scraped_count)
                    elif scraped_count > 0:
                        # Too few stocks: the DripInvesting.org page shape likely changed.
                        # Skip the DB replace so we don't wipe good data with a broken scrape.
                        logger.error(
                            "Scrape returned only %s stocks (< %s expected); skipping DB update to "
                            "avoid wiping good data. DripInvesting.org page structure may have changed.",
                            scraped_count, min_expected)
                    else:
                        logger.warning("No data scraped.")

            except Exception as e:
                logger.exception("Error during update: %s", e)
                mysql_connection.conn.rollback()

            # Enrich every ticker with fresh Yahoo Finance data (prices etc.) and
            # record when that enrichment ran, so it stays current even on days the
            # DripInvesting.org scrape is skipped.
            if configuration["scrape_yahoo_finance"] is True:
                tickers_list = mysql_connection.get_tickers_from_db()
                mysql_connection.update_metadata_table({"yahoo_finance": get_current_datetime_string()})
                yahoo_data = get_yahoo_finance_data_for_tickers_list(tickers_list)
                mysql_connection.update_data_table(yahoo_data)

        # add a random delay between runs, if zero there will be none
        random_delay(configuration["max_random_delay_seconds"], stop_event=_stop_event)

    logger.info("Shutdown requested, exiting.")
