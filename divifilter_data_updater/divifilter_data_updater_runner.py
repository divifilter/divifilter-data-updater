from divifilter_data_updater.drip_investing_scraper import *
from divifilter_data_updater.configure import *
from divifilter_data_updater.helper_functions import *
from divifilter_data_updater.db_functions import *
from divifilter_data_updater.yahoo_finance import *
from divifilter_data_updater.finviz_data import *


def init():
    while True:
        configuration = read_configurations()

        scraper = DripInvestingScraper()

        # disable yahoo spammy logs if set
        if configuration["disable_yahoo_logs"] is True:
            disable_yahoo_logs()

        with MysqlConnection(configuration["mysql_uri"]) as mysql_connection:
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

