import pandas as pd


class forward_filling:
    def __init__(self, forward_filling_input_):
        self.forward_filling_input = forward_filling_input_
        self.forward_filling_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.forward_filling_input = self.forward_filling_input.replace(
                -1, pd.NA)
            self.forward_filling_output = self.forward_filling_input.ffill()
            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close', 'volume']
            self.forward_filling_output[columns_to_convert] = self.forward_filling_output[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')
            return self.forward_filling_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
