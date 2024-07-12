import pandas as pd
from scipy.interpolate import KroghInterpolator, interp1d, PchipInterpolator, Akima1DInterpolator


class BaseInterpolation:
    def __init__(self, interpolation_input):
        self.interpolation_input = interpolation_input.replace(-1, pd.NA)
        self.interpolation_output = self.interpolation_input.copy()
        self.columns_to_interpolate = [
            col for col in self.interpolation_input.columns if col != 'volume']
        self.process_filling()

    def interpolate_column(self, series):
        raise NotImplementedError("Subclasses should implement this!")

    def process_filling(self):
        try:
            for column in self.columns_to_interpolate:
                series = self.interpolation_output[column]
                interpolated_values = self.interpolate_column(series)
                self.interpolation_output[column] = series.fillna(
                    pd.Series(interpolated_values, index=series.index))

            # Apply forward filling for the 'volume' column
            self.interpolation_output['volume'] = self.interpolation_output['volume'].fillna(
                method='ffill')

            # Convert the numeric columns to float again
            self.interpolation_output[self.columns_to_interpolate + ['volume']
                                      ] = self.interpolation_output[self.columns_to_interpolate + ['volume']].apply(pd.to_numeric, errors='coerce')
        except Exception as e:
            print(f"An error occurred: {e}")


class KroghInterpolation(BaseInterpolation):
    def interpolate_column(self, series):
        non_na_indices = series.dropna().index
        non_na_values = series.dropna().values
        interpolator = KroghInterpolator(non_na_indices, non_na_values)
        return interpolator(series.index)


class ZeroInterpolation(BaseInterpolation):
    def interpolate_column(self, series):
        non_na_indices = series.dropna().index
        non_na_values = series.dropna().values
        interpolator = interp1d(
            non_na_indices, non_na_values, kind='zero', fill_value="extrapolate")
        return interpolator(series.index)


class PiecewisePolynomialInterpolation(BaseInterpolation):
    def interpolate_column(self, series):
        # PiecewisePolynomial interpolation is deprecated in scipy, using Pchip instead
        non_na_indices = series.dropna().index
        non_na_values = series.dropna().values
        interpolator = PchipInterpolator(non_na_indices, non_na_values)
        return interpolator(series.index)


class PchipInterpolation(BaseInterpolation):
    def interpolate_column(self, series):
        non_na_indices = series.dropna().index
        non_na_values = series.dropna().values
        interpolator = PchipInterpolator(non_na_indices, non_na_values)
        return interpolator(series.index)


class AkimaInterpolation(BaseInterpolation):
    def interpolate_column(self, series):
        non_na_indices = series.dropna().index
        non_na_values = series.dropna().values
        interpolator = Akima1DInterpolator(non_na_indices, non_na_values)
        return interpolator(series.index)
