import pandas as pd
from statsmodels.tsa.arima.model import ARIMA


class arima_imputation:
    def __init__(self, arima_imputation_input_, order=(1, 1, 1)):
        self.arima_imputation_input = arima_imputation_input_
        self.order = order
        self.arima_imputation_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.arima_imputation_input = self.arima_imputation_input.replace(
                -1, pd.NA)
            self.arima_imputation_output = self.arima_imputation_input.copy()
            columns_to_impute = [
                col for col in self.arima_imputation_input.columns if col != 'volume']

            for column in columns_to_impute:
                series = self.arima_imputation_output[column]
                model = ARIMA(series, order=self.order)
                model_fit = model.fit()
                imputed_values = model_fit.predict(start=0, end=len(series)-1)
                self.arima_imputation_output[column] = series.fillna(
                    pd.Series(imputed_values, index=series.index))

            # Apply forward filling for the 'volume' column
            self.arima_imputation_output['volume'] = self.arima_imputation_output['volume'].fillna(
                method='ffill')

            # Convert the numeric columns to float again
            self.arima_imputation_output[columns_to_impute + [
                'volume']] = self.arima_imputation_output[columns_to_impute + ['volume']].apply(pd.to_numeric, errors='coerce')
            return self.arima_imputation_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
