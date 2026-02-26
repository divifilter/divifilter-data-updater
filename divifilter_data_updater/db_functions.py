from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, text, select, inspect
import pandas as pd
import math
import re


class MysqlConnection:

    def __init__(self, uri_string: str):
        # check due to pymsql requiring changes to the URI string that are often forgetten
        if not uri_string:
            raise ValueError("MYSQL_URI must be set in configurations or environment variables")
            
        if "pymysql" not in uri_string:
            uri_string = uri_string.replace("mysql", "mysql+pymysql", 1)
            
        connect_args = {}
        if "aivencloud.com" in uri_string:
            connect_args['ssl'] = {'ssl_mode': 'REQUIRED'}
            
        self.engine = create_engine(uri_string, connect_args=connect_args, echo=False)
        self.conn = self.engine.connect()
        self.meta = MetaData()
        self.dividend_update_times = Table(
            'dividend_update_times', self.meta,
            Column('name', String(32), primary_key=True, unique=True),
            Column('last_update_time', String(32))
        )

    def close(self):
        self.conn.close()
        self.engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def update_data_table_from_data_frame(self, data_table_to_update: pd.DataFrame):
        dtype_map = {
            'Symbol': String(32),
            'No Years': Float,
            'Price': Float,
            'Div Yield': Float,
            '5Y Avg Yield': Float,
            'Current Div': Float,
            'Annualized': Float,
            'Low': Float,
            'High': Float,
            'DGR 1Y': Float,
            'DGR 3Y': Float,
            'DGR 5Y': Float,
            'DGR 10Y': Float,
            'TTR 1Y': Float,
            'TTR 3Y': Float,
            'FV %': Float,
            'Fair Value': Float,
            'Chowder Number': Float,
            'Revenue 1Y': Float,
            'NPM': Float,
            'CF/Share': Float,
            'ROE': Float,
            'Debt/Capital': Float,
            'ROTC': Float,
            'P/E': Float,
            'P/BV': Float,
            'PEG': Float,
            'Market Cap': Float
        }
        staging_table = "dividend_data_table_staging"
        data_table_to_update.to_sql(staging_table, con=self.engine, if_exists="replace", index=False, dtype=dtype_map)

        with self.engine.connect() as conn:
            # Ensure staging table has a primary key on Symbol for ON DUPLICATE KEY UPDATE
            conn.execute(text(f"ALTER TABLE `{staging_table}` ADD PRIMARY KEY (`Symbol`)"))

            main_exists = self.engine.dialect.has_table(conn, "dividend_data_table")
            if not main_exists:
                # First run: rename (nothing reading yet)
                conn.execute(text(f"RENAME TABLE {staging_table} TO dividend_data_table"))
            else:
                # Ensure main table has a primary key on Symbol (fix for tables created without one)
                result = conn.execute(text("SHOW KEYS FROM `dividend_data_table` WHERE Key_name = 'PRIMARY'"))
                if not result.fetchone():
                    # Table has no PK, likely has duplicates. Drop and replace with staging.
                    conn.execute(text("DROP TABLE `dividend_data_table`"))
                    conn.execute(text(f"RENAME TABLE `{staging_table}` TO `dividend_data_table`"))
                    conn.commit()
                    return

                staging_cols = [col['name'] for col in inspect(self.engine).get_columns(staging_table)]
                main_cols = {col['name'] for col in inspect(self.engine).get_columns("dividend_data_table")}

                # Add any new columns to main (rare; ALTER is brief)
                for col in staging_cols:
                    if col not in main_cols:
                        conn.execute(text(f"ALTER TABLE `dividend_data_table` ADD COLUMN `{col}` TEXT"))

                col_list = ", ".join(f"`{c}`" for c in staging_cols)
                update_clause = ", ".join(
                    f"`{c}` = VALUES(`{c}`)" for c in staging_cols if c != 'Symbol'
                )
                conn.execute(text(
                    f"INSERT INTO `dividend_data_table` ({col_list}) "
                    f"SELECT {col_list} FROM `{staging_table}` "
                    f"ON DUPLICATE KEY UPDATE {update_clause}"
                ))
                conn.execute(text(
                    f"DELETE m FROM `dividend_data_table` m "
                    f"LEFT JOIN `{staging_table}` s ON m.Symbol = s.Symbol "
                    f"WHERE s.Symbol IS NULL"
                ))
                conn.execute(text(f"DROP TABLE `{staging_table}`"))
            conn.commit()

    def update_metadata_table(self, time_dict_to_update: dict):
        self.meta.create_all(self.conn, tables=[self.dividend_update_times])
        for timestamp_key, timestamp_value in time_dict_to_update.items():
            query = text(
                "INSERT INTO dividend_update_times (name, last_update_time) "
                "VALUES (:name, :update_time) "
                "ON DUPLICATE KEY UPDATE last_update_time = :update_time"
            )
            self.conn.execute(query, {"name": timestamp_key, "update_time": timestamp_value})
            self.conn.commit()

    def run_sql_query(self, sql_query):
        query = text(sql_query)
        result = self.conn.execute(query)
        if "SELECT" in query.text.upper():
            data = result.fetchall()
        else:
            data = None
            self.conn.commit()
        return data

    def check_db_update_dates(self):
        if not self.engine.dialect.has_table(self.conn, "dividend_update_times"):
            self.dividend_update_times.create(self.conn)
        data_dict = dict(self.conn.execute(select(self.dividend_update_times)).fetchall())
        return data_dict

    @staticmethod
    def _safe_param_name(column_name):
        """Convert column name to a safe SQL parameter name."""
        return re.sub(r'[^a-zA-Z0-9_]', '_', column_name)

    def update_data_table(self, finviz_data: tuple):
        """
        Update the 'dividend_data_table' with enrichment data (Yahoo/Finviz).
        Batches column checks and per-ticker UPDATEs for performance.

        Args:
            finviz_data (tuple): Tuple containing a timestamp and a dictionary of data for tickers.
        """
        # Check if the 'dividend_data_table' exists; if not, create it
        if not self.engine.dialect.has_table(self.conn, "dividend_data_table"):
            self.create_dividend_data_table()

        if not (isinstance(finviz_data, tuple) and len(finviz_data) == 2):
            raise ValueError("finviz_data must be a tuple containing a timestamp and a dictionary")

        timestamp, ticker_data = finviz_data

        if not ticker_data:
            return

        # 1. Collect all column names and add missing ones in one pass
        all_columns = set()
        for data in ticker_data.values():
            all_columns.update(data.keys())

        existing_columns = {col['name'] for col in inspect(self.engine).get_columns("dividend_data_table")}
        for col in all_columns - existing_columns:
            self.add_column_to_table("dividend_data_table", col)

        # 2. Batch update: one UPDATE per ticker instead of per cell
        for symbol, data in ticker_data.items():
            updates = {}
            for col, val in data.items():
                if val is None:
                    continue
                if isinstance(val, (int, float)) and math.isnan(val):
                    continue
                updates[col] = val

            if not updates:
                continue

            set_clauses = ", ".join(
                f"`{col}` = COALESCE(:{self._safe_param_name(col)}, `{col}`)"
                for col in updates
            )
            params = {self._safe_param_name(col): val for col, val in updates.items()}
            params["symbol"] = symbol

            query = text(f"UPDATE dividend_data_table SET {set_clauses} WHERE Symbol = :symbol")
            self.conn.execute(query, params)

        self.conn.commit()

    def create_dividend_data_table(self):
        """
        Create the 'dividend_data_table' if it does not exist.

        Returns:
            None
        """
        dividend_data_table = Table(
            'dividend_data_table', self.meta,
            Column('Symbol', String(32), primary_key=True),
            Column('Company', String(255)),
            Column('Sector', String(255)),
            Column('No Years', Integer),
            Column('Price', Float),
            Column('Div Yield', Float),
            Column('5Y Avg Yield', Float),
            Column('Current Div', Float),
            Column('Payouts/ Year', String(32)),
            Column('Annualized', Float),
            Column('Low', Float),
            Column('High', Float),
            Column('DGR 1Y', Float),
            Column('DGR 3Y', Float),
            Column('DGR 5Y', Float),
            Column('DGR 10Y', Float),
            Column('TTR 1Y', Float),
            Column('TTR 3Y', Float),
            Column('Fair Value', Float),
            Column('FV %', Float),
            Column('Chowder Number', Float),
            Column('Revenue 1Y', Float),
            Column('NPM', Float),
            Column('CF/Share', Float),
            Column('ROE', Float),
            Column('Debt/Capital', Float),
            Column('ROTC', Float),
            Column('P/E', Float),
            Column('P/BV', Float),
            Column('PEG', Float),
            Column('Industry', String(255)),
            Column('Market Cap', Float),
            Column('Price Change', String(32)),
            Column('Price Change %', String(32)),
            Column('Next Earnings Report', String(64)),
            Column('Previous Div', String(32)),
            Column('Ex-date', String(32)),
            Column('Dividend Pay Date', String(32)),
            Column('TTR 1Y - With Specials', String(32)),
            Column('TTR 3Y - With Specials', String(32)),
            Column('TTR 1Y - No Specials', String(32)),
            Column('TTR 3Y - No Specials', String(32)),
            Column('Fair Value (P/E 10)', String(32)),
            Column('FV (P/E 10) %', String(32)),
            Column('Fair Value (Peter Lynch)', String(32)),
            Column('FV (Peter Lynch) %', String(32)),
            Column('Current R', String(32)),
            Column('TTR 1Y', String(32)),
            Column('TTR 3Y', String(32))
        )

        if not inspect(self.conn).has_table("dividend_data_table"):
            dividend_data_table.create(self.conn)

    def get_tickers_from_db(self):
        """
        Retrieve a list of tickers from the 'dividend_data_table' in the database.

        Returns:
            list: List of tickers.
        """
        # Check if the 'dividend_data_table' exists; if not, return an empty list
        if not inspect(self.conn).has_table("dividend_data_table"):
            return []

        # Create a SQL query to select all distinct tickers from the table
        query = "SELECT DISTINCT Symbol FROM dividend_data_table;"

        # Execute the query and fetch the results
        result = self.run_sql_query(query)

        # Extract the tickers from the result set
        tickers = [row[0] for row in result]

        return tickers

    def column_exists(self, table_name, column_name):
        inspector = inspect(self.engine)
        return column_name in [col['name'] for col in inspector.get_columns(table_name)]

    def add_column_to_table(self, table_name, column_name):
        # Enclose column name with backticks to handle spaces
        column_name_with_backticks = f"`{column_name}`"
        self.run_sql_query(f'ALTER TABLE {table_name} ADD COLUMN {column_name_with_backticks} VARCHAR(255);')

