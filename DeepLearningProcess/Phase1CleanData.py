from datetime import datetime
import pandas as pd
import numpy as np

null_values = [np.nan, "NaN", "NULL", "", None, "null", "NULL"]
list_columns = ['stock_id', 'date',
                'open', 'high', 'low', 'close', 'volume']

list_of_cleaning_data_method_with_stock = ["interpolate", "ffill", "bfill", "mean", "median", "mode",
                                           "zero", "spline", "polynomial", "krogh", "piecewise_polynomial", "pchip", "akima", "cubicspline"]


def retrieve_dataframe_unit(symbol=None):
    """
    This is the function to retrieve_dataframe_unit corresponding to the day

    Date Range: ('2014-01-01' -> Today)

    """
    # Create a date range from 01-01-2014 to today
    start_date = '2014-01-01'
    date_range = pd.date_range(
        start=start_date, end=datetime.today().strftime('%Y-%m-%d'))

    # Step 2: Create a DataFrame with this date range and other columns based
    # on table struture set to -1
    default_df = pd.DataFrame(date_range, columns=["date"])
    default_df['stock_id'] = symbol
    for col in list_columns[2:]:
        default_df[col] = "-1"
    return default_df


class phase_1_clean_data():
    def __init__(self, list_of_symbols, dataframe_input):
        # Check if the list of symbols is empty
        if len(list_of_symbols) == 0:
            raise ValueError("list_of_symbols is empty")

        # Assign the mode to process the data based on the number of symbols input
        self.input_processing_mode = "all" if len(
            list_of_symbols) > 1 else "single"
        self.list_of_cleaning_data_method_available = list_of_cleaning_data_method_with_stock
        # Assign the input data to the data to be processed
        # Single stock data processing
        if len(list_of_symbols) == 1 and len(dataframe_input) != 0:
            self.process_data = dataframe_input
            # Clean the null, nan, "", NaN, None values inside the dataframe
            self.data_null_nan_cleanned = self.clean_data_null_nan(
                self.process_data)

            # Fill the missing data
            self.dataframe_unit = retrieve_dataframe_unit(list_of_symbols[0])

            # Merge the data_null_nan_cleanned with the dataframe_unit
            self.data_null_nan_cleanned['date'] = pd.to_datetime(
                self.data_null_nan_cleanned['date'])
            self.dataframe_unit['date'] = pd.to_datetime(
                self.dataframe_unit['date'])

            # Fill the missing data
            self.data_prepare_fill_missing = self.merge_single_stock_processes(
                self.data_null_nan_cleanned, self.dataframe_unit)

        # Multiple stock data processing
        elif len(list_of_symbols) > 1 and len(dataframe_input) != 0:
            self.dict_process_data = self.process_input_dataframes(list_of_symbols,
                                                                   dataframe_input)

            # Clean the null, nan, "", NaN, None values inside the dataframe
            self.data_null_nan_cleanned = self.clean_data_null_nan(
                self.dict_process_data)

            # Fill the missing data

        else:
            raise ValueError(
                "list_of_symbols is empty or dataframe_input is empty")

    def process_input_dataframes(self, list_of_symbols, dataframe_input):
        """
        This function will process the input is the combine dataframe from all the stock data
        """
        dict_process_data = {}
        for symbol in list_of_symbols:
            dict_process_data[symbol] = dataframe_input[dataframe_input['stock_id'] == symbol]
        return dict_process_data

    def clean_data_null_nan(self, process_data):
        """
        This function will clean the null, nan, "", NaN, None values inside the dataframe
        """
        if self.input_processing_mode == "single":
            # Clean the null, nan, "", NaN, None values inside the dataframe
            for col in process_data.columns:
                process_data[col] = process_data[col].replace(
                    null_values, np.nan)
            return process_data.dropna()

        elif self.input_processing_mode == "all":
            dict_data_null_nan_cleanned = {}
            # Loop through the dictionary of dataframes
            for symbol, data in self.dict_process_data.items():
                # Clean the null, nan, "", NaN, None values inside the dataframe
                for col in data.columns:
                    data[col] = data[col].replace(
                        null_values, np.nan)
                # Drop the rows with NaN values and store it in the dictionary
                dict_data_null_nan_cleanned[symbol] = data.dropna()
            return dict_data_null_nan_cleanned

        else:
            return None

    def merge_single_stock_processes(self, stock_df, date_unit_df):
        merged_df = pd.merge(stock_df, date_unit_df[['date', 'stock_id', 'open', 'high', 'low', 'close', 'volume']],
                             on=['date', 'stock_id'],
                             how='right',
                             suffixes=('_df1', '_df2'))

        # Replace NaN values in df1 columns with corresponding values from df2
        for column in ['open', 'high', 'low', 'close', 'volume']:
            merged_df[column + '_df1'] = merged_df[column +
                                                   '_df1'].fillna(merged_df[column + '_df2'])

        # Select the relevant columns
        filled_df = merged_df[['date', 'stock_id', 'open_df1',
                               'high_df1', 'low_df1', 'close_df1', 'volume_df1']]

        # Rename the columns to their original names
        filled_df.columns = ['date', 'stock_id',
                             'open', 'high', 'low', 'close', 'volume']
        filled_df.loc[:, 'date'] = pd.to_datetime(
            filled_df['date'], format="%Y-%m-%d")
        return filled_df

    def process_filling_dataframe_with_method(self, method):
        """
        This function will process the filling of the missing data in the dataframe
        """
        if method not in self.list_of_cleaning_data_method_available:
            return "The method is not available in the list_of_cleaning_data_method_available"

        if self.input_processing_mode == "single":
            # Fill the missing data
            if method == "interpolate":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "ffill":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    method=method)
            elif method == "bfill":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    method=method)
            elif method == "mean":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    self.data_prepare_fill_missing.mean())
            elif method == "median":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    self.data_prepare_fill_missing.median())
            elif method == "mode":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    self.data_prepare_fill_missing.mode().iloc[0])
            elif method == "zero":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.fillna(
                    0)
            elif method == "spline":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "polynomial":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "krogh":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "piecewise_polynomial":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "pchip":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "akima":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            elif method == "cubicspline":
                self.data_prepare_fill_missing = self.data_prepare_fill_missing.interpolate(
                    method=method)
            else:
                raise ValueError(
                    "The method is not available in the list_of_cleaning_data_method_available")
            return self.data_prepare_fill_missing
