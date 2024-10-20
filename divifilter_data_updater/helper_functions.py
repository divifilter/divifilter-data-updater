import pandas as pd
from datetime import datetime
import random
import time


def radar_dict_to_table(radar_dict: dict) -> pd.DataFrame:
    """
    Takes a dict of the radar file and returns it as a dataframe to display in streamlit

    :param radar_dict: The dict of the data to work with

    :return radar_df: The dict but in table/dataframe form
    """
    radar_df = pd.DataFrame.from_dict(radar_dict, orient='index')
    return radar_df


def remove_unneeded_columns(radar_dict: dict, unneeded_column_list: list) -> dict:
    """
    Takes a dict of the radar file and returns it with the unneeded columns removed

    :param radar_dict: The dict of the data to work with
    :param unneeded_column_list: list of columns to remove

    :return radar_dict: The dict with the keys in unneeded_column_list removed from it
    """
    for key_to_remove in unneeded_column_list:
        for dict_value in radar_dict.values():
            dict_value.pop(key_to_remove, None)
    return radar_dict


def get_current_datetime_string():
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")
    return date_time


def random_delay(max_delay_time: int):
    """
    sleep a random amount of time

    :param max_delay_time: Max time in seconds it is ok to delay
    """
    delay_time = random.randint(0, max_delay_time)
    print("will now sleep for " + str(delay_time) + " seconds")
    time.sleep(delay_time)
