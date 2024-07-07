# stock_database_manager.py
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os

load_dotenv(override=True)

create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")
postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")

class StockDatabaseManager:
    def __init__(self):
        self.dbname = postgres_dbname
        self.user = postgres_user
        self.password = postgres_pass
        self.host = postgres_server
        self.port = postgres_port
        self.conn = self.create_connection()

    def create_connection(self):
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as e:
            print(e)
            return None

    def create_schema_and_tables(self, tickers):
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_schema_query)
            for ticker in tickers:
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS tickets.{ticker} ("
                    "stock_id VARCHAR(10),"
                    "date VARCHAR(10),"
                    "open VARCHAR(50),"
                    "high VARCHAR(50),"
                    "low VARCHAR(50),"
                    "close VARCHAR(50),"
                    "volume VARCHAR(50),"
                    "PRIMARY KEY (stock_id, date))"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS {ticker}_date_idx ON tickets.{ticker} (date)"
                )
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def insert_data(self, ticker, data):
        try:
            cursor = self.conn.cursor()
            data = data.astype(str)
            insert_query = (
                f"INSERT INTO tickets.{ticker} "
                "(stock_id, date, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (stock_id, date) DO UPDATE SET "
                "open = EXCLUDED.open, "
                "high = EXCLUDED.high, "
                "low = EXCLUDED.low, "
                "close = EXCLUDED.close, "
                "volume = EXCLUDED.volume"
            )
            for _, row in data.iterrows():
                cursor.execute(insert_query, (row['stock_id'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume']))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print(e)

    def get_data_by_table(self, table_name):
        try:
            query = f"SELECT * FROM tickets.{table_name}"
            data = pd.read_sql(query, self.conn)
            return data
        except Exception as e:
            print(e)
            return None

    def get_tables(self, schema='tickets'):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s", (schema,)
            )
            tables = cursor.fetchall()
            cursor.close()
            return [table[0] for table in tables]
        except Exception as e:
            print(e)
            return None

    def fetch_all_data(self, schema='tickets'):
        try:
            tables = self.get_tables(schema)
            all_data = {}
            for table in tables:
                query = f"SELECT * FROM {schema}.{table}"
                df = pd.read_sql(query, self.conn)
                all_data[table] = df
            return all_data
        except Exception as e:
            print(e)
            return None

    def close_connection(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                print(e)
