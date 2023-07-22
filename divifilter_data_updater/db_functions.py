from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
import pandas as pd
from datetime import datetime


class MysqlConnection:

    def __init__(self, uri_string: str):
        self.engine = create_engine(uri_string, echo=False)

    def update_data_table_from_data_frame(self, data_table_to_update: pd.DataFrame):
        data_table_to_update.to_sql("dividend_data_table", con=self.engine, if_exists="replace", index=False)

    def update_metadata_table(self, radar_timestamp: datetime, finviz_timestamp: datetime):
        with self.engine.connect() as conn:
            meta = MetaData()

            dividend_update_times = Table(
                'dividend_update_times', meta,
                Column('name', String(32), primary_key=True),
                Column('last_update_time', DateTime)
            )
            meta.create_all(conn, tables=[dividend_update_times])
            conn.execute(dividend_update_times.insert(), [
                {'name': 'radar_file', 'last_update_time': radar_timestamp},
                {'name': 'finviz', 'last_update_time': finviz_timestamp}
            ])

