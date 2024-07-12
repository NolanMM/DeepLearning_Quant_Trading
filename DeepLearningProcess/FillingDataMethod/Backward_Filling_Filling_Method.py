import pandas as pd


class backward_filling:
    def __init__(self, backward_filling_input_):
        self.backward_filling_input = backward_filling_input_
        self.backward_filling_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.backward_filling_input = self.backward_filling_input.replace(
                -1, pd.NA)

            # Apply backward fill to all columns except 'volume'
            columns_to_bfill = self.backward_filling_input.columns.difference([
                                                                              'volume'])
            self.backward_filling_input[columns_to_bfill] = self.backward_filling_input[columns_to_bfill].bfill(
            )

            # Apply forward fill to the 'volume' column
            self.backward_filling_input['volume'] = self.backward_filling_input['volume'].ffill(
            )

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.backward_filling_input[columns_to_convert] = self.backward_filling_input[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            self.backward_filling_output = self.backward_filling_input

            return self.backward_filling_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
