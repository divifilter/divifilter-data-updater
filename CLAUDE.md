# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dividend stock data updater that scrapes data from DripInvesting.org, enriches it with Yahoo Finance data, and stores it in a MySQL database. Runs as a containerized service deployed to Northflank.

## Commands

```bash
# Run tests
python -m unittest

# Run tests with coverage
coverage run -m unittest
coverage report

# Run a single test file
python -m unittest test.test_drip_investing_scraper

# Run a single test case
python -m unittest test.test_drip_investing_scraper.TestDripInvestingScraper.test_get_tickers

# Lint (CI uses flake8)
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Install dependencies
pip install -r requirements.txt

# Run the updater
python update_divifilter_data.py
```

## Architecture

**Data flow:** `DripInvestingScraper` → dict list → DataFrame → MySQL (`dividend_data_table`, replaced each run) → Yahoo Finance enrichment (updates individual cells via SQL UPDATE).

- **Entry point:** `update_divifilter_data.py` → `divifilter_data_updater_runner.py:init()` which loops indefinitely with a configurable random delay between runs.
- **DripInvestingScraper** (`drip_investing_scraper.py`): Paginates through dripinvesting.org/stocks/ to discover tickers, then scrapes individual stock pages in parallel (ThreadPoolExecutor, 10 workers). Uses thread-local `requests.Session` instances with retry adapters.
- **db_functions.py**: `MysqlConnection` wraps SQLAlchemy. `update_data_table_from_data_frame()` does a full table replace via `to_sql(if_exists="replace")`. `update_data_table()` does per-cell updates with COALESCE for Yahoo/Finviz data.
- **configure.py**: Uses `parse_it` to load config from `config/` directory or environment variables. Key config: `mysql_uri` (required), `scrape_yahoo_finance`, `scrape_finviz`, `max_random_delay_seconds`.
- **helper_functions.py**: `clean_numeric_value()` strips formatting ($, %, commas, M/B/x suffixes) and converts to float. Used by all scrapers.
- **Finviz integration** is currently disabled (upstream library broken).

## Key Patterns

- Numeric columns must be explicitly mapped in `dtype_map` in `db_functions.py:update_data_table_from_data_frame()` to maintain Float types in MySQL.
- The scraper maps site-specific labels to canonical column names (e.g., "High (52W)" → "High", "Fair Value (Blended)" → "Fair Value"). Unmapped labels pass through via the `else` clause.
- Tests mock `requests.Session` at the module level (`@patch('divifilter_data_updater.drip_investing_scraper.requests.Session')`).

## CI/CD

Push to `main` triggers: lint → test (Python 3.12-3.14) → Docker build (amd64/arm64) → push to DockerHub → deploy to Northflank. PRs only run lint + tests.
