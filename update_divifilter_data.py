import logging

from divifilter_data_updater.divifilter_data_updater_runner import init

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init()
