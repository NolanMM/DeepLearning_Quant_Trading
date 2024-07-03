import yfinance as yf
import os
import sys
import pandas as pd

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Add the specific path to the BatchProcess module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../BatchProcess')))

from StockDatabaseManager import StockDatabaseManager
from DataSource.ListSnP500.ListSnP500Collect import ListSAndP500

def fetch_stock_data(ticker, period='1y'):
    stock_data = yf.download(ticker, period=period)
    if stock_data.empty:
        print(f"No data fetched for {ticker}")
    else:
        print(f"Data fetched for {ticker}:")
        print(stock_data.head())
    stock_data.reset_index(inplace=True)
    stock_data['stock_id'] = ticker
    stock_data.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    }, inplace=True)
    stock_data = stock_data[['stock_id', 'date', 'open', 'high', 'low', 'close', 'volume']]
    return stock_data

def main():
    db_manager = StockDatabaseManager()
    tickers = ListSAndP500().tickers_list
    db_manager.create_schema_and_tables(tickers)

    all_cleaned_data = pd.DataFrame()
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        data = fetch_stock_data(ticker)

        cleaned_data = db_manager.interpolate_data(data)
        db_manager.insert_data(ticker, cleaned_data)
        all_cleaned_data = pd.concat([all_cleaned_data, cleaned_data], ignore_index=True)

    all_cleaned_data.to_csv('interpolated_stock_data.csv', index=False)
    db_manager.close_connection()

if __name__ == "__main__":
    main()
