from parse_it import ParseIt


def read_configurations(config_folder: str = "config") -> dict:
    """
    Will create a config dict that includes all of the configurations for terraformize by aggregating from all valid
    config sources (files, envvars, cli args, etc) & using sane defaults on config params that are not declared

    Arguments:
        :param config_folder: the folder which all configuration file will be read from recursively

    Returns:
        :return config: a dict of all configurations needed for terraformize to work
    """
    print("reading config variables")

    config = {}
    parser = ParseIt(config_location=config_folder, recurse=True)
    config["dividend_radar_download_url"] = \
        parser.read_configuration_variable("dividend_radar_download_url",
                                           default_value="https://www.dripinvesting.org/stocks/")
    # config["local_file_path"] = parser.read_configuration_variable("local_file_path",
    #                                                                default_value="/tmp/latest_dividend_radar.xlsx")
    config["mysql_uri"] = parser.read_configuration_variable("mysql_uri")
    config["scrape_yahoo_finance"] = parser.read_configuration_variable("scrape_yahoo_finance", default_value=True)
    config["scrape_finviz"] = parser.read_configuration_variable("scrape_finviz", default_value=True)
    config["disable_yahoo_logs"] = parser.read_configuration_variable("disable_yahoo_logs", default_value=True)
    config["max_random_delay_seconds"] = parser.read_configuration_variable("max_random_delay_seconds", default_value=3600)
    return config
