import pandas as pd


class moving_average_filling_method:
    def __init__(self, moving_average_input_, windows_input, min_period_input):
        self.moving_average_input = moving_average_input_
        self.windows = windows_input
        self.min_period = min_period_input
        self.moving_average_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.moving_average_input = self.moving_average_input.replace(
                -1, pd.NA)

            # Create a copy of the input dataframe for the output
            self.moving_average_output = self.moving_average_input.copy()

            # Apply moving average to all columns except 'volume'
            for column in self.moving_average_output.columns:
                if column != 'volume':
                    self.moving_average_output[column] = self.moving_average_input[column].fillna(
                        self.moving_average_input[column].rolling(
                            window=self.windows, min_periods=self.min_period).mean()
                    )

            # Forward fill the remaining NA values
            self.moving_average_output = self.moving_average_output.fillna(
                method='ffill')

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.moving_average_output[columns_to_convert] = self.moving_average_output[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            return self.moving_average_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
