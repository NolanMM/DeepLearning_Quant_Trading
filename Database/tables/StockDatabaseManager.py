from dotenv import load_dotenv
import pandas as pd
import psycopg2
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(override=True)

# Load environment variables
create_schema_query = os.getenv("CREATE_SCHEMA_QUERY")
create_reddit_schema_query = os.getenv("CREATE_REDDIT_SCHEMA_QUERY")
create_reddit_table_query = os.getenv("CREATE_REDDIT_TABLE_QUERY")
configure_reddit_table = os.getenv("CONFIGURE_REDDIT_TABLE")
insert_query_reddit_table = os.getenv("INSERT_QUERY_REDDIT_TABLE")
postgres_server = os.getenv("DATABASE_SERVER")
postgres_port = os.getenv("DATABASE_PORT")
postgres_dbname = os.getenv("DATABASE_NAME")
postgres_user = os.getenv("DATABASE_USER")
postgres_pass = os.getenv("DATABASE_PASSWORD")

class StockDatabaseManager:
    def __init__(self):
        """
        Initialize the database connection
        """
        self.dbname = postgres_dbname
        self.user = postgres_user
        self.password = postgres_pass
        self.host = postgres_server
        self.port = postgres_port
        self.conn = self.create_connection()

    def create_connection(self):
        """
        Create a connection to the database
        """
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
            logger.error("Error creating database connection: %s", e)
            return None

    def create_schema_and_tables(self, tickers):
        """
        Create the schema and tables for the given tickers list
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_schema_query)
                cursor.execute(create_reddit_schema_query)
                cursor.execute(create_reddit_table_query)
                cursor.execute(configure_reddit_table)

                for ticker in tickers:
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS tickets." + ticker + " ("
                        "stock_id VARCHAR(10),"
                        "date DATE,"
                        "open NUMERIC,"
                        "high NUMERIC,"
                        "low NUMERIC,"
                        "close NUMERIC,"
                        "volume INTEGER,"
                        "PRIMARY KEY (stock_id, date))"
                    )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS " + ticker +
                        "_date_idx ON tickets." + ticker + " (date)"
                    )

                self.conn.commit()
        except Exception as e:
            logger.error("Error creating schema and tables: %s", e)
    
    def interpolate_data(self, data):
        """
        Interpolate missing data in the DataFrame
        """
        try:
            data = data.set_index('date')
            data = data.astype(float)  # Ensure all columns are float
            data = data.interpolate(method='linear')
            data.reset_index(inplace=True)
            return data
        except Exception as e:
            logger.error("Error interpolating data: %s", e)
            return None

    def insert_data(self, ticker, data):
        """
        Insert data into the database
        """
        try:
            with self.conn.cursor() as cursor:
                data = data.astype(str)
                insert_query = (
                    "INSERT INTO tickets." + ticker +
                    " (stock_id, date, open, high, low, close, volume)"
                    " VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    " ON CONFLICT (stock_id, date) DO UPDATE SET"
                    " open = EXCLUDED.open,"
                    " high = EXCLUDED.high,"
                    " low = EXCLUDED.low,"
                    " close = EXCLUDED.close,"
                    " volume = EXCLUDED.volume"
                )

                for index, row in data.iterrows():
                    cursor.execute(insert_query, (row['stock_id'], row['date'],
                                                  row['open'], row['high'], row['low'], row['close'], row['volume']))
                self.conn.commit()
        except Exception as e:
            logger.error("Error inserting data into database: %s", e)

    def insert_reddit_data(self, data):
        """
        Insert Reddit data into the database
        """
        try:
            with self.conn.cursor() as cursor:
                data = data.astype(str)
                for index, row in data.iterrows():
                    cursor.execute(insert_query_reddit_table, (row['id'], row['subreddit'],
                                                              row['url'], row['title'],
                                                              row['score'], row['num_comments'],
                                                              row['downvotes'], row['ups'],
                                                              row['date_created_utc']))
                self.conn.commit()
        except Exception as e:
            logger.error("Error inserting Reddit data into database: %s", e)

    def get_data_by_table(self, table_name):
        """
        Get data from the given ticket table
        """
        try:
            query = f"SELECT * FROM tickets.{table_name}"
            data = pd.read_sql(query, self.conn)
            return pd.DataFrame(data)  # Ensure it is a DataFrame
        except Exception as e:
            logger.error("Error fetching data from table %s: %s", table_name, e)
            return None

    def get_tables(self, schema='tickets'):
        """
        Get all tables in the given schema
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s", (schema,)
                )
                tables = cursor.fetchall()
                return [table[0] for table in tables]
        except Exception as e:
            logger.error("Error fetching tables from schema %s: %s", schema, e)
            return None

    def fetch_all_data(self, schema='tickets'):
        """
        Fetch all data from the given schema
        """
        try:
            tables = self.get_tables(schema)
            all_data = {}
            for table in tables:
                query = "SELECT * FROM " + schema + "." + table
                df = pd.read_sql(query, self.conn)
                all_data[table] = pd.DataFrame(df)  # Ensure it is a DataFrame
            return all_data
        except Exception as e:
            logger.error("Error fetching all data from schema %s: %s", schema, e)
            return None

    def close_connection(self):
        """
        Close the database connection
        """
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logger.error("Error closing database connection: %s", e)
