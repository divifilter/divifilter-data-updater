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
        
        mysql_connection = MysqlConnection(configuration["mysql_uri"])
        
        # We might not need this check depending on how strict we want to be, 
        # but previously it checked for file version. 
        # Now we'll just act as if we need to update.
        # mysql_update_dates = mysql_connection.check_db_update_dates()

        # disable yahoo spammy logs if set
        if configuration["disable_yahoo_logs"] is True:
            disable_yahoo_logs()

        try:
            print("Starting scrape from DripInvesting.org...")
            # Scrape data
            scraped_data_list = scraper.scrape_all_data()
            
            if scraped_data_list:
                # Convert list to dict format expected by helper functions (ticker -> data)
                radar_dict = {item['Symbol']: item for item in scraped_data_list}
                
                # Filter columns if needed (scraper already selects relevant data, but we can ensure cleanup)
                # The previous unneeded_columns might not apply to new data keys, but keeping clean
                unneeded_columns = ["FV", "None", None, "Ex-Date", "Pay-Date"] 
                # Note: "Current R", "New Member" etc might not exist in new data
                
                radar_dict_filtered = remove_unneeded_columns(radar_dict, unneeded_columns)
                
                # Convert to dataframe and update DB
                mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))
                
                # Update metadata
                # Using a timestamp as version since looking up a "version string" from site is harder
                mysql_connection.update_metadata_table({"radar_file": get_current_datetime_string()})
                print("Database updated successfully.")
            else:
                print("No data scraped.")

        except Exception as e:
            print(f"Error during update: {e}")
            # Logic to handle failure? Retrying is built into the loop.

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

