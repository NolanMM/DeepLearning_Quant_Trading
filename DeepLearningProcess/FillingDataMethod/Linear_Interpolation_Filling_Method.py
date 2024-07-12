import pandas as pd


class linear_interpolation:
    def __init__(self, linear_interpolation_input_):
        self.linear_interpolation_input = linear_interpolation_input_
        self.linear_interpolation_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.linear_interpolation_input = self.linear_interpolation_input.replace(
                -1, pd.NA)

            # Apply linear interpolation to all columns except 'volume'
            columns_to_interpolate = self.linear_interpolation_input.columns.difference([
                                                                                        'volume'])
            self.linear_interpolation_input[columns_to_interpolate] = self.linear_interpolation_input[columns_to_interpolate].interpolate(
                method='linear')

            # Apply forward fill to the 'volume' column
            self.linear_interpolation_input['volume'] = self.linear_interpolation_input['volume'].ffill(
            )

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.linear_interpolation_input[columns_to_convert] = self.linear_interpolation_input[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            self.linear_interpolation_output = self.linear_interpolation_input

            return self.linear_interpolation_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
