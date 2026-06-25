from divifilter_data_updater.drip_investing_scraper import *
from divifilter_data_updater.configure import *
from divifilter_data_updater.helper_functions import *
from divifilter_data_updater.db_functions import *
from divifilter_data_updater.yahoo_finance import *
from divifilter_data_updater.finviz_data import *
import time


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
            print(f"Database connection attempt {attempt}/{max_attempts} failed: {e}; retrying in {delay}s")
            time.sleep(delay)
            attempt += 1


def init():
    while True:
        configuration = read_configurations()

        scraper = DripInvestingScraper()

        # disable yahoo spammy logs if set
        if configuration["disable_yahoo_logs"] is True:
            disable_yahoo_logs()

        try:
            database_connection = _connect_with_retry(configuration["mysql_uri"])
        except Exception as e:
            print(f"Could not connect to the database, will retry next cycle: {e}")
            random_delay(configuration["max_random_delay_seconds"])
            continue

        with database_connection as mysql_connection:
            try:
                print("Starting scrape from DripInvesting.org...")
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

                    # Update metadata
                    mysql_connection.update_metadata_table({"radar_file": get_current_datetime_string()})
                    print("Database updated successfully.")
                else:
                    print("No data scraped.")

            except Exception as e:
                print(f"Error during update: {e}")
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
                #if configuration["scrape_finviz"] is True:
                #    mysql_connection.update_metadata_table({"finviz": get_current_datetime_string()})
                #    finviz_data = get_finviz_data_for_tickers_list(tickers_list)
                #    mysql_connection.update_data_table(finviz_data)

        # add a random delay between runs, if zero there will be non
        random_delay(configuration["max_random_delay_seconds"])

