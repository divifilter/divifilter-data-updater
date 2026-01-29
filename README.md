# divifilter-data-updater
Filter dividend stocks easily

Github actions CI unit tests & auto dockerhub push status: [![CI/CD](https://github.com/divifilter/divifilter-data-updater/actions/workflows/full_ci_cd_workflow.yml/badge.svg)](https://github.com/divifilter/divifilter-data-updater/actions/workflows/full_ci_cd_workflow.yml)

Code coverage: [![codecov](https://codecov.io/gh/divifilter/divifilter-data-updater/branch/main/graph/badge.svg?token=ZB0FX61FU9)](https://codecov.io/gh/divifilter/divifilter-data-updater)

## Data Source

This project scrapes dividend stock data from [DripInvesting.org](https://www.dripinvesting.org/stocks/), which provides comprehensive information on dividend-paying stocks including Dividend Kings, Champions, Contenders, and Challengers.

**Note:** The previous data source (Portfolio Insight Excel file) is no longer maintained and has been replaced with web scraping from DripInvesting.org.

## Features

- Automated scraping of 100+ dividend stocks
- Multi-threaded data collection for improved performance
- Integration with Yahoo Finance for additional market data
- MySQL database storage
- Configurable update intervals

