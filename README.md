# divifilter-data-updater

Filter dividend stocks easily

[![CI/CD](https://github.com/divifilter/divifilter-data-updater/actions/workflows/full_ci_cd_workflow.yml/badge.svg)](https://github.com/divifilter/divifilter-data-updater/actions/workflows/full_ci_cd_workflow.yml)
[![codecov](https://codecov.io/gh/divifilter/divifilter-data-updater/branch/main/graph/badge.svg?token=ZB0FX61FU9)](https://codecov.io/gh/divifilter/divifilter-data-updater)

## Description

A data pipeline that fetches dividend stock data from multiple sources and stores it in a MySQL database. It runs in a continuous loop, periodically checking for new data and updating the database.

## Data Sources

- **DripInvesting.org** — Scrapes dividend stock data including Dividend Kings, Champions, Contenders, and Challengers.
- **Yahoo Finance** — Scrapes additional financial data (price, market cap, P/E ratio, etc.) for each ticker.

**Note:** The previous data source (Portfolio Insight Excel file) is no longer maintained and has been replaced with web scraping from DripInvesting.org.

## Usage — Running with Python

Install dependencies:

```bash
pip install -r requirements.txt
```

Run:

```bash
python update_divifilter_data.py
```

## Usage — Running with Docker

Build and run:

```bash
docker build -t divifilter-data-updater .
docker run -e mysql_uri="mysql+pymysql://user:pass@host/db" divifilter-data-updater
```

Or use the pre-built image:

```bash
docker pull naorlivne/divifilter-data-updater
docker run -e mysql_uri="mysql+pymysql://user:pass@host/db" naorlivne/divifilter-data-updater
```

## Configuration

Configuration is managed via environment variables, CLI arguments, or config files in a `config/` directory (using [parse_it](https://github.com/naorlivne/parse_it)).

| Variable | Description | Default |
|---|---|---|
| `mysql_uri` | MySQL connection URI (SQLAlchemy format) | **required** |
| `dividend_radar_download_url` | URL to download the dividend radar spreadsheet | `https://www.portfolio-insight.com/dividend-radar` |
| `local_file_path` | Local path to store the downloaded radar file | `/tmp/latest_dividend_radar.xlsx` |
| `scrape_yahoo_finance` | Enable Yahoo Finance data scraping | `true` |
| `scrape_finviz` | Enable Finviz data scraping | `true` |
| `disable_yahoo_logs` | Suppress verbose Yahoo Finance logs | `true` |
| `max_random_delay_seconds` | Max random delay (in seconds) between loop iterations | `3600` |

## Testing

Lint:

```bash
flake8 . --count --max-complexity=10 --max-line-length=127 --statistics
```

Run tests:

```bash
coverage run -m unittest
```

Coverage report:

```bash
coverage report
```

Supported Python versions: 3.12, 3.13, 3.14

## Related Projects

- [divifilter-ui](https://github.com/divifilter/divifilter-ui) — The web UI that displays the data harvested by this pipeline.

## License

[LGPL-3.0](LICENSE)
