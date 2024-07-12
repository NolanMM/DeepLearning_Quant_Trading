import pandas as pd


class polynomial_interpolation:
    def __init__(self, polynomial_interpolation_input_, order_input_):
        self.polynomial_interpolation_input = polynomial_interpolation_input_
        self.order = order_input_
        self.polynomial_interpolation_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.polynomial_interpolation_input = self.polynomial_interpolation_input.replace(
                -1, pd.NA)

            # Apply polynomial interpolation to all columns except 'volume'
            columns_to_interpolate = self.polynomial_interpolation_input.columns.difference([
                                                                                            'volume'])
            self.polynomial_interpolation_input[columns_to_interpolate] = self.polynomial_interpolation_input[columns_to_interpolate].interpolate(
                method='polynomial', order=self.order)

            # Apply forward fill to the 'volume' column
            self.polynomial_interpolation_input['volume'] = self.polynomial_interpolation_input['volume'].ffill(
            )

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.polynomial_interpolation_input[columns_to_convert] = self.polynomial_interpolation_input[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            self.polynomial_interpolation_output = self.polynomial_interpolation_input

            return self.polynomial_interpolation_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
