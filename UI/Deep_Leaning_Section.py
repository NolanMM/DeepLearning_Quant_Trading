from DeepLearningProcess.Phase1CleanData import phase_1_clean_data
from BatchProcess.BatchProcess import BatchProcessManager
import streamlit as st


@st.cache_data(ttl=1800)
def batch_process_retrieve_data_by_stock(the_stock_in):
    return BatchProcessManager().get_stock_data_by_ticker(the_stock_in)


@st.cache_data(ttl=1800)
def batch_process_retrieve_all_data_in_stock_table():
    return BatchProcessManager().get_all_stock_data_in_database()


@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv().encode("utf-8")


class deep_learning_model_section():
    def __init__(self, _list_of_symbols, deep_learning_tab):
        self.deep_learning_tabs = deep_learning_tab
        self.list_of_symbols = _list_of_symbols

    def run(self):
        with self.deep_learning_tabs:
            st.markdown("## I. Deep Learning Section")
            st.write("This section is under construction")
            raw_data_retrieve = self.step_1_retrieve_raw_data_section()
            if raw_data_retrieve is not None:
                self.step_2_data_preparation_section(raw_data_retrieve)

    def step_1_retrieve_raw_data_section(self):
        st.markdown("### 1. Retrieve the data for deep learning")
        if "stock_data_deep_learning" not in st.session_state:
            st.session_state.stock_data_deep_learning = None
        the_stock_deep_learning = st.selectbox(
            "Select the stock you want to retrieve from database (if available)", self.list_of_symbols, key="deep_learning")
        btn_prepare_deep_learning = st.button(
            "Retrieve Single Stock Data from Database")

        btn_retrieve_all_data_deep_learning = st.button(
            "Retrieve All Stock Data from Database")

        if btn_prepare_deep_learning:
            st.session_state.stock_data_deep_learning = the_stock_deep_learning
        elif btn_retrieve_all_data_deep_learning:
            st.session_state.stock_data_deep_learning = "all"

        raw_data_retrieve = None
        if st.session_state.stock_data_deep_learning == the_stock_deep_learning:
            raw_data_retrieve = batch_process_retrieve_data_by_stock(
                st.session_state.stock_data_deep_learning)
        elif st.session_state.stock_data_deep_learning == "all":
            raw_data_retrieve = batch_process_retrieve_all_data_in_stock_table()

        if raw_data_retrieve is not None:
            # Print raw data
            st.write(raw_data_retrieve)
            total_raw_rows_data = len(raw_data_retrieve)
            st.success("The number of rows total inside raw dataframe is: " +
                       str(total_raw_rows_data))
            num_rows_with_issues_before_clean = len(
                raw_data_retrieve[
                    raw_data_retrieve.isnull().any(axis=1) |
                    raw_data_retrieve.isna().any(axis=1) |
                    (raw_data_retrieve == '').any(axis=1)
                ]
            )
            message_rows_with_issues_before_clean = "The number of rows with null, nan, '', NaN, None values inside dataframe is: "
            if num_rows_with_issues_before_clean/total_raw_rows_data > 0.5:
                st.error(message_rows_with_issues_before_clean +
                         str(num_rows_with_issues_before_clean))
            elif num_rows_with_issues_before_clean/total_raw_rows_data > 0.1:
                st.warning(message_rows_with_issues_before_clean +
                           str(num_rows_with_issues_before_clean))
            else:
                st.success(message_rows_with_issues_before_clean +
                           str(num_rows_with_issues_before_clean))
            return raw_data_retrieve
        else:
            return None

    def step_2_data_preparation_section(self, raw_data_retrieve):
        total_row_message = "The number of rows total inside dataframe is: "
        st.markdown("### 2. Data Preparation")
        phase_1_clean_data_ = phase_1_clean_data(
            [st.session_state.stock_data_deep_learning], raw_data_retrieve)

        ############################################################################################
        # Step 2.1
        st.markdown(
            "#### 2.1 Data Cleaning Null, Nan, '', NaN, None values inside the dataframe")
        st.write(phase_1_clean_data_.data_null_nan_cleanned)
        st.success(total_row_message +
                   str(len(phase_1_clean_data_.data_null_nan_cleanned)))
        num_rows_with_issues_after_clean = len(
            phase_1_clean_data_.data_null_nan_cleanned[
                phase_1_clean_data_.data_null_nan_cleanned.isnull().any(axis=1) |
                phase_1_clean_data_.data_null_nan_cleanned.isna().any(axis=1) |
                (phase_1_clean_data_.data_null_nan_cleanned == '').any(axis=1)
            ]
        )
        st.success("The number of rows with null, nan, '', NaN, None values inside dataframe is: " +
                   str(num_rows_with_issues_after_clean))
        ############################################################################################
        # Step 2.2
        st.markdown(
            "#### 2.2 Final Dataset ready for filling the -1 values with the selected method")
        st.write(phase_1_clean_data_.data_prepare_fill_missing)
        st.success(total_row_message +
                   str(len(phase_1_clean_data_.data_prepare_fill_missing)))
        num_rows_with_issues_after_clean = len(
            phase_1_clean_data_.data_prepare_fill_missing[
                phase_1_clean_data_.data_prepare_fill_missing.isnull().any(axis=1) |
                phase_1_clean_data_.data_prepare_fill_missing.isna().any(axis=1) |
                (phase_1_clean_data_.data_prepare_fill_missing == '-1').any(axis=1)
            ]
        )
        st.success("The number of rows with null, nan, '', NaN, None values inside dataframe is: " +
                   str(num_rows_with_issues_after_clean))
        ############################################################################################
        # Step 2.3
        st.markdown("#### 2.3 Data Cleaning Method")
        data_cleaning_method_deep_learning_selectbox = st.selectbox(
            "Select the method you want to process to fill the -1 values", phase_1_clean_data_.list_of_cleaning_data_method_available, key="cleaning_data_method_deep_learning")
        btn_process_data_cleaning_using_method_deep_learning = st.button(
            "Processing...")

        if "cleaning_data_method" not in st.session_state:
            st.session_state.cleaning_data_method = None
        if btn_process_data_cleaning_using_method_deep_learning:
            st.session_state.cleaning_data_method = data_cleaning_method_deep_learning_selectbox

        if st.session_state.cleaning_data_method is not None:
            st.write(
                f"Data Cleaning Method: {st.session_state.cleaning_data_method}")

        ############################################################################################
