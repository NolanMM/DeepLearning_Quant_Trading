from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

load_dotenv(override=True)

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
        self.engine = self.create_engine()

    def create_engine(self):
        try:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
            return engine
        except Exception as e:
            print(e)
            return None

    def create_schema_and_tables(self, tickers):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS tickets"))
                for ticker in tickers:
                    conn.execute(text(
                        f"CREATE TABLE IF NOT EXISTS tickets.{ticker} ("
                        "stock_id VARCHAR(10),"
                        "date DATE,"
                        "open FLOAT,"
                        "high FLOAT,"
                        "low FLOAT,"
                        "close FLOAT," 
                        "volume BIGINT,"
                        "PRIMARY KEY (stock_id, date))"
                    ))
                    conn.execute(text(
                        f"CREATE INDEX IF NOT EXISTS {ticker}_date_idx ON tickets.{ticker} (date)"
                    ))
        except Exception as e:
            print(e)

    def insert_data(self, ticker, data):
        try:
            data = data.astype(str)
            insert_query = (
                f"INSERT INTO tickets.{ticker} "
                "(stock_id, date, open, high, low, close, volume) "
                "VALUES (:stock_id, :date, :open, :high, :low, :close, :volume) "
                "ON CONFLICT (stock_id, date) DO UPDATE SET "
                "open = EXCLUDED.open, "
                "high = EXCLUDED.high, "
                "low = EXCLUDED.low, "
                "close = EXCLUDED.close, "
                "volume = EXCLUDED.volume"
            )

            with self.engine.connect() as conn:
                for _, row in data.iterrows():
                    conn.execute(text(insert_query), {
                        'stock_id': row['stock_id'],
                        'date': row['date'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
        except Exception as e:
            print(e)

    def get_data_by_table(self, table_name):
        try:
            query = f"SELECT * FROM tickets.{table_name}"
            data = pd.read_sql(query, self.engine)
            return data
        except Exception as e:
            print(e)
            return None

    def get_tables(self, schema='tickets'):
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s"
            tables = pd.read_sql(query, self.engine, params=(schema,))
            return tables['table_name'].tolist()
        except Exception as e:
            print(e)
            return None

    def fetch_all_data(self, schema='tickets'):
        try:
            tables = self.get_tables(schema)
            all_data = {}
            for table in tables:
                query = f"SELECT * FROM {schema}.{table}"
                df = pd.read_sql(query, self.engine)
                all_data[table] = df
            return all_data
        except Exception as e:
            print(e)
            return None

    def fill_missing_dates(self, data, ticker):
        start_date = '2014-01-01'
        end_date = datetime.today().strftime('%Y-%m-%d')
        date_range = pd.date_range(start=start_date, end=end_date)
        default_df = pd.DataFrame(date_range, columns=['date'])
        default_df['stock_id'] = ticker
        default_df['open'] = None
        default_df['high'] = None
        default_df['low'] = None
        default_df['close'] = None
        default_df['volume'] = None

        # Convert date columns to datetime
        default_df['date'] = pd.to_datetime(default_df['date'])
        data['date'] = pd.to_datetime(data['date'])

        merged_df = pd.merge(default_df, data, on=['date', 'stock_id'], how='left', suffixes=('_default', ''))
        return merged_df

    def handle_missing_values(self, data, method='mean'):
        data_to_fill = data[['open', 'high', 'low', 'close', 'volume']].copy()
        
        # Ensure the columns to fill are numeric
        data_to_fill = data_to_fill.apply(pd.to_numeric, errors='coerce')

        if method == 'mean':
            data_to_fill = data_to_fill.fillna(data_to_fill.rolling(window=31, min_periods=1, center=True).mean())
        elif method == 'ffill':
            data_to_fill = data_to_fill.fillna(method='ffill')
        elif method == 'bfill':
            data_to_fill = data_to_fill.fillna(method='bfill')
        elif method == 'interpolate':
            data_to_fill = data_to_fill.interpolate(method='linear')

        data.update(data_to_fill)
        # Convert numeric columns back to float
        for col in ['open', 'high', 'low', 'close', 'volume']:
            data[col] = data[col].astype(float)
        return data

    def eda(self, data, ticker, method):
        print(f"\nBasic EDA for {ticker} after handling missing values with {method}:\n")
        print("Data Types:")
        print(data.dtypes)
        print("\nSummary Statistics:")
        print(data.describe())

    def save_to_csv(self, data, filename):
        data.to_csv(filename, index=False)
        print(f"Saved cleaned data to {filename}")

    def save_to_excel(self, data, filename):
        data.to_excel(filename, index=False)
