# StockDatabaseManagerTest.py
from StockDatabaseManager import StockDatabaseManager
import pandas as pd

def main():
    # Initialize the StockDatabaseManager
    manager = StockDatabaseManager()

    # Test schema and table creation
    tickers = ['AAPL', 'GOOGL', 'MSFT']
    manager.create_schema_and_tables(tickers)

    # Create a sample dataframe
    sample_data = {
        'stock_id': ['AAPL', 'AAPL'],
        'date': ['2022-07-01', '2022-07-02'],
        'open': ['135.0', '137.0'],
        'high': ['140.0', '142.0'],
        'low': ['133.0', '135.0'],
        'close': ['138.0', '140.0'],
        'volume': ['1000000', '1500000']
    }
    df = pd.DataFrame(sample_data)

    # Insert data into the database
    manager.insert_data('AAPL', df)

    # Fetch and print data
    data = manager.get_data_by_table('AAPL')
    print(data)

    # Close the connection
    manager.close_connection()

if __name__ == "__main__":
    main()
