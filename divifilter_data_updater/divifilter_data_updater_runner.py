from divifilter_data_updater.dividend_radar import *
from divifilter_data_updater.configure import *
from divifilter_data_updater.helper_functions import *
from divifilter_data_updater.db_functions import *
#from yahoo_finance import *
#from finviz_data import *


def init():
    configuration = read_configurations()

    radar_file = DividendRadar(
        dividend_radar_url=configuration["dividend_radar_download_url"],
        local_file=configuration["local_file_path"]
    )

    mysql_connection = MysqlConnection(configuration["mysql_uri"])
    mysql_update_dates = mysql_connection.check_db_update_dates()

    try:
        # if not the latest version of the radar file update to it
        if mysql_update_dates["radar"] != radar_file.find_latest_version():
            if radar_file.check_if_local_is_latest() is False:
                radar_file.download_latest_version()
            starting_radar_dict = radar_file.read_radar_file_to_dict()
            radar_dict_filtered = starting_radar_dict
            unneeded_columns = ["FV", "None", None, "Current R", "New Member", "Previous Div", "Streak Basis",
                                "Ex-Date", "Pay-Date"]
            radar_dict_filtered = remove_unneeded_columns(radar_dict_filtered, unneeded_columns)
            mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))
            mysql_connection.update_metadata_table({"radar": radar_file.latest_local_version})
    # if there was a problem let's recreate all the data to be sure
    except Exception:
        radar_file.download_latest_version()
        starting_radar_dict = radar_file.read_radar_file_to_dict()
        radar_dict_filtered = starting_radar_dict
        unneeded_columns = ["FV", "None", None, "Current R", "New Member", "Previous Div", "Streak Basis", "Ex-Date",
                            "Pay-Date"]
        radar_dict_filtered = remove_unneeded_columns(radar_dict_filtered, unneeded_columns)
        mysql_connection.update_data_table_from_data_frame(radar_dict_to_table(radar_dict_filtered))
        mysql_connection.update_metadata_table({"radar": radar_file.latest_local_version})


    # TODO - finish creating the functions to update the db based on finviz data, no need to check if latest as it is
    # always updated, just update all tickers and then update the timetable with teh finviz update date to be later
    # also shown to enduser
    mysql_connection.update_metadata_table({"finviz": get_current_datetime_string()})
