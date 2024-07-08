import sys
import os
from pathlib import Path

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
# Add the specific path to the BatchProcess module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../BatchProcess')))

from StockDatabaseManager import StockDatabaseManager
import pandas as pd
from datetime import datetime
from DataSource.ListSnP500.ListSnP500Collect import ListSAndP500

def main():
    manager = StockDatabaseManager()
    tickers = ListSAndP500().tickers_list
    manager.create_schema_and_tables(tickers)

    all_cleaned_data = pd.DataFrame()

    # Load, clean, and save data
    for ticker in tickers:
        print(f"Processing data for {ticker}...")
        data = manager.get_data_by_table(ticker)
        
        if data is not None:
            merged_df = manager.fill_missing_dates(data, ticker)
            print(f"Missing values for {ticker} before handling:")
            print(merged_df.isna().sum())

            method = 'mean'  # Choose the desired method for filling missing values
            print(f"\nHandling missing values using {method} method for {ticker}...")
            filled_df = manager.handle_missing_values(merged_df.copy(), method)
            print(f"Missing values for {ticker} after handling with {method}:")
            print(filled_df.isna().sum())

            filled_df['ticker'] = ticker

            all_cleaned_data = pd.concat([all_cleaned_data, filled_df])

            manager.eda(filled_df, ticker, method)
            print("\n" + "-"*50 + "\n")

    # Save the combined data to a single CSV file
    manager.save_to_csv(all_cleaned_data, 'SP500_cleaned_data.csv')

    print("All cleaned data saved to SP500_cleaned_data.csv")

    manager.close_connection()

if __name__ == "__main__":
    main()
