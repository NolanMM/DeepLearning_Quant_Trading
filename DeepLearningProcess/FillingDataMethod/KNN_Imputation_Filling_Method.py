import pandas as pd
from sklearn.impute import KNNImputer


class knn_imputation:
    def __init__(self, knn_imputation_input_, n_neighbors=5):
        self.knn_imputation_input = knn_imputation_input_
        self.n_neighbors = n_neighbors
        self.knn_imputation_output = self.process_filling()

    def process_filling(self):
        try:
            # Replace all -1 values placeholder with NA in pandas
            self.knn_imputation_input = self.knn_imputation_input.replace(
                -1, pd.NA)

            # Apply forward fill for the 'volume' column
            self.knn_imputation_input['volume'] = self.knn_imputation_input['volume'].ffill(
            )

            # Apply KNN imputation for all other columns
            imputer = KNNImputer(n_neighbors=self.n_neighbors)
            columns_to_impute = self.knn_imputation_input.columns.difference([
                                                                             'volume'])
            imputed_data = imputer.fit_transform(
                self.knn_imputation_input[columns_to_impute])
            self.knn_imputation_output = self.knn_imputation_input.copy()
            self.knn_imputation_output[columns_to_impute] = imputed_data

            # Convert the numeric columns to float again
            columns_to_convert = ['open', 'high', 'low', 'close']
            self.knn_imputation_output[columns_to_convert] = self.knn_imputation_output[columns_to_convert].apply(
                pd.to_numeric, errors='coerce')

            return self.knn_imputation_output
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
