from divifilter_data_updater.dividend_radar import *
from divifilter_data_updater.configure import *
from divifilter_data_updater.helper_functions import *
from divifilter_data_updater.db_functions import *
from divifilter_data_updater.yahoo_finance import *
from divifilter_data_updater.finviz_data import *


def init():
    while True:
        configuration = read_configurations()

        radar_file = DividendRadar(
            dividend_radar_url=configuration["dividend_radar_download_url"],
            local_file=configuration["local_file_path"]
        )

        mysql_connection = MysqlConnection(configuration["mysql_uri"])
        mysql_update_dates = mysql_connection.check_db_update_dates()

        # disable yahoo spammy logs if set
        if configuration["disable_yahoo_logs"] is True:
            disable_yahoo_logs()

        try:
            # if not the latest version of the radar file update to it
            if mysql_update_dates["radar_file"] != radar_file.find_latest_version():
                if radar_file.check_if_local_is_latest() is False:
                    radar_file.download_latest_version()
                starting_radar_dict = radar_file.read_radar_file_to_dict()
                radar_dict_filtered = starting_radar_dict
                unneeded_columns = ["FV", "None", None, "Current R", "New Member", "Previous Div", "Streak Basis",
                                    "Ex-Date", "Pay-Date"]
                radar_dict_filtered = remove_unneeded_columns(radar_dict_filtered, unneeded_columns)
                mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))
                mysql_connection.update_metadata_table({"radar_file": radar_file.latest_local_version})
        # if there was a problem let's recreate all the data to be sure
        except Exception:
            radar_file.download_latest_version()
            starting_radar_dict = radar_file.read_radar_file_to_dict()
            radar_dict_filtered = starting_radar_dict
            unneeded_columns = ["FV", "None", None, "Current R", "New Member", "Previous Div", "Streak Basis", "Ex-Date",
                                "Pay-Date"]
            radar_dict_filtered = remove_unneeded_columns(radar_dict_filtered, unneeded_columns)
            mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))
            mysql_connection.update_metadata_table({"radar_file": radar_file.latest_local_version})

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
