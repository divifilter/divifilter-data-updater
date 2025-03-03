import yfinance as yf
from retrying import retry
from datetime import datetime, timezone
import requests
import logging
import json


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=10)
def get_yahoo_finance_data_for_tickers_tuple(tickers_tuple: tuple) -> tuple[datetime, dict]:
    """
    Takes a tuple of tickers and returns the relevant data for them from yahoo_finance, have to use tuple because of
    caching hating lists

    :param tickers_tuple: the tuple of tickers you want the data for

    :return yahoo_finance_query_date_time: the date and time in UTC yahoo finance was queried at
    :return filtered_radar_dict: A dict including all data for tickers requested
    """

    filtered_radar_dict = {}
    tickers = yf.Tickers(list(tickers_tuple))

    for stock_ticker in tickers_tuple:
        wanted_stock_dict = {
            "Price": "currentPrice",
            "Low": "fiftyTwoWeekLow",
            "High": "fiftyTwoWeekHigh",
            "P/BV": "priceToBook",
            "Payout Ratio": "payoutRatio"
        }
        filtered_radar_dict[stock_ticker] = {}
        for wanted_stock_key, wanted_stock_value in wanted_stock_dict.items():
            try:
                filtered_radar_dict[stock_ticker][wanted_stock_key] = \
                    tickers.tickers[stock_ticker.upper()].info[wanted_stock_value]
            # this is ugly but yahoo finance does not like dots in the stock names and replace them inconsistently
            except KeyError:
                dot_ticker = yf.Ticker(stock_ticker.replace(".", "-"))
                try:
                    filtered_radar_dict[stock_ticker][wanted_stock_key] = dot_ticker.info[wanted_stock_value]
                except KeyError:
                    dot_ticker = yf.Ticker(stock_ticker.replace(".", ""))
                    try:
                        filtered_radar_dict[stock_ticker][wanted_stock_key] = dot_ticker.info[wanted_stock_value]
                    except KeyError:
                        pass
            except AttributeError:
                pass
            except TypeError:
                pass
            except requests.exceptions.HTTPError:
                pass
            except json.decoder.JSONDecodeError:
                pass
    yahoo_finance_query_date_time = datetime.now(timezone.utc)
    return yahoo_finance_query_date_time, filtered_radar_dict


def get_yahoo_finance_data_for_tickers_list(tickers_list: list) -> tuple[datetime, dict]:
    """
    A wrapper for get_yahoo_finance_data_for_tickers_tuple, only difference is it takes a list and turns it to tuple as
    an ugly but simple workaround for cache not liking lists

    :param tickers_list: the list of tickers you want the data for

    :return yahoo_finance_query_date_time: the date and time in UTC yahoo finance was queried at
    :return filtered_radar_dict: A dict including all data for tickers requested
    """

    return get_yahoo_finance_data_for_tickers_tuple(tuple(tickers_list))


def disable_yahoo_logs():
    logger = logging.getLogger('yfinance')
    logger.disabled = True
    logger.propagate = False
