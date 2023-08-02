from sqlalchemy import create_engine, MetaData, Table, Column, String, text, select
import pandas as pd


class MysqlConnection:

    def __init__(self, uri_string: str):
        self.engine = create_engine(uri_string, echo=False)
        self.conn = self.engine.connect()
        self.meta = MetaData()
        self.dividend_update_times = Table(
            'dividend_update_times', self.meta,
            Column('name', String(32), primary_key=True, unique=True),
            Column('last_update_time', String(32))
        )

    def update_data_table_from_data_frame(self, data_table_to_update: pd.DataFrame):
        data_table_to_update.to_sql("dividend_data_table", con=self.engine, if_exists="replace", index=False)

    def update_metadata_table(self, time_dict_to_update: dict):
        self.meta.create_all(self.conn, tables=[self.dividend_update_times])
        for timestamp_key, timestamp_value in time_dict_to_update.items():
            self.run_sql_query("INSERT INTO dividend_update_times (name, last_update_time) VALUES ('" + timestamp_key +
                               "', '" + timestamp_value + "') ON DUPLICATE KEY UPDATE last_update_time = '"
                               + timestamp_value + "';")

    def run_sql_query(self, sql_query):
        query = text(sql_query)
        result = self.conn.execute(query)
        if query.is_select:
            data = result.fetchall()
        else:
            data = None
        self.conn.commit()
        return data

    def check_db_update_dates(self):
        data_dict = dict(self.conn.execute(select(self.dividend_update_times)).fetchall())
        return data_dict
