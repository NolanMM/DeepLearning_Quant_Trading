import pandas as pd


class mean_15_days_filling:
    def __init__(self, df_mean_15_day_input_):
        self.df_mean_15_day_input = df_mean_15_day_input_
        self.df_mean_15_day_output = self.process_filling()

    def process_filling(self):
        try:
            for index, row in self.df_mean_15_day_input.iterrows():
                for col in self.df_mean_15_day_input.columns:
                    if pd.isna(row[col]):
                        if col == 'volume':
                            # Apply forward fill for volume column
                            self.df_mean_15_day_input.at[index, col] = self.df_mean_15_day_input['volume'].ffill(
                            ).at[index]
                        else:
                            # Replace NA values with mean value for other columns
                            mean_value = self.get_mean_15_days(
                                row['date'], col)
                            self.df_mean_15_day_input.at[index,
                                                         col] = mean_value
            return self.df_mean_15_day_input
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_mean_15_days(self, current_date, col_name):
        start_date = current_date - pd.Timedelta(days=15)
        mask = (self.df_mean_15_day_input['date'] < current_date) & (
            self.df_mean_15_day_input['date'] >= start_date)
        return self.df_mean_15_day_input.loc[mask, col_name].mean()
