import pandas as pd
from datetime import datetime
import random
import time
import logging

logger = logging.getLogger(__name__)


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


def random_delay(max_delay_time: int, stop_event=None):
    """
    sleep a random amount of time

    :param max_delay_time: Max time in seconds it is ok to delay
    :param stop_event: optional threading.Event; when provided the delay is
        interruptible and returns early if the event is set (graceful shutdown)
    """
    delay_time = random.randint(0, max_delay_time)
    logger.info("will now sleep for %s seconds", delay_time)
    if stop_event is not None:
        stop_event.wait(delay_time)
    else:
        time.sleep(delay_time)

# Magnitude suffixes used for large numbers (e.g. Market Cap "1.5B"). These must
# be scaled, not just stripped, otherwise "1.5B" becomes 1.5 instead of 1.5e9.
_MAGNITUDE_MULTIPLIERS = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}


# Conservative sanity bounds (low, high); None means unbounded on that side.
# Values outside these are treated as obviously-bad data and nulled before persist.
_VALIDATION_BOUNDS = {
    "Price": (0, None),          # a real stock price is positive
    "Div Yield": (0, 100),       # a yield above 100% is a data glitch
    "5Y Avg Yield": (0, 100),
}


def validate_radar_data(radar_dict: dict) -> dict:
    """
    Null out implausible numeric values in-place (conservative bounds only) so
    obviously-bad scraped data doesn't reach the DB. Logs each correction.

    :param radar_dict: ticker -> {column: value} mapping (modified in place)
    :return radar_dict: the same dict, with out-of-range values set to None
    """
    for symbol, record in radar_dict.items():
        for field, (low, high) in _VALIDATION_BOUNDS.items():
            value = record.get(field)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (ValueError, TypeError):
                continue
            if (low is not None and numeric < low) or (high is not None and numeric > high):
                logger.warning("Dropping out-of-range %s=%s for %s", field, value, symbol)
                record[field] = None
    return radar_dict


def clean_numeric_value(value):
    """
    Cleans a string value and converts it to a numeric type.
    Handles percentages, dollar signs, commas, the 'x' multiple marker (P/E etc.),
    and scales magnitude suffixes (K/M/B/T) used for large numbers like Market Cap.
    Returns None if conversion fails.
    """
    if value is None or value == '' or value == 'N/A':
        return None

    # Remove formatting characters: currency, thousands separators, percent, and the
    # 'x' marker used for ratios. Magnitude suffixes (K/M/B/T) are handled separately
    # below so they can be scaled rather than discarded.
    cleaned = str(value).strip()
    cleaned = cleaned.replace('$', '').replace(',', '').replace('%', '')
    cleaned = cleaned.replace('x', '').replace('X', '').strip()

    # Handle special cases
    if cleaned in ['', '-', 'N/A', 'None']:
        return None

    multiplier = 1
    if cleaned[-1].upper() in _MAGNITUDE_MULTIPLIERS:
        multiplier = _MAGNITUDE_MULTIPLIERS[cleaned[-1].upper()]
        cleaned = cleaned[:-1].strip()

    try:
        return round(float(cleaned) * multiplier, 2)
    except (ValueError, AttributeError):
        return None
