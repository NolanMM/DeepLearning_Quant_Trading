import pandas as pd


class time_based_interpolation:
    def __init__(self, time_based_interpolation_input_):
        self.time_based_interpolation_input = time_based_interpolation_input_
        self.time_based_interpolation_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.time_based_interpolation_input = self.time_based_interpolation_input.replace(
                -1, pd.NA)

            # Apply time-based interpolation to all columns except 'volume'
            columns_to_interpolate = self.time_based_interpolation_input.columns.difference([
                                                                                            'volume'])
            self.time_based_interpolation_input[columns_to_interpolate] = self.time_based_interpolation_input[columns_to_interpolate].interpolate(
                method='time')

            # Apply forward fill to the 'volume' column
            self.time_based_interpolation_input['volume'] = self.time_based_interpolation_input['volume'].ffill(
            )

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.time_based_interpolation_input[columns_to_convert] = self.time_based_interpolation_input[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            self.time_based_interpolation_output = self.time_based_interpolation_input

            return self.time_based_interpolation_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
