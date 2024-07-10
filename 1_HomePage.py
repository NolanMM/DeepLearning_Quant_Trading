from BatchProcess.DataSource.ListSnP500.ListSnP500Collect import ListSAndP500
from DeepLearningProcess.Phase1CleanData import phase_1_clean_data
from BatchProcess.BatchProcess import BatchProcessManager
from UI.Set_Up_Section import set_up_section
from multiprocessing.pool import ThreadPool
import plotly.graph_objects as go
from dotenv import load_dotenv
from pathlib import Path
import streamlit as st
import pandas as pd
import time
import os

pool = ThreadPool(processes=6)
load_dotenv(override=True)

current_dir = Path(__file__).parent if "__file__" in locals() else Path.cwd()
css_file = current_dir / os.getenv("CSS_DIR")
defaut_start_date = "2014-01-01"

st.set_page_config(page_title="Home Page", page_icon=":computer:",
                   initial_sidebar_state="collapsed")
st.sidebar.header("Quantitative Trading Project")
st.title("Deep Learning Quant Trading")
st.markdown(
    """
        <style>
            .st-emotion-cache-ocqkz7.e1f1d6gn5{
            text-align: center;
            }

            h1{
                text-align: center;
            }

            .st-emotion-cache-13ln4jf.ea3mdgi5 {
                max-width: 1200px;
            }
        </style>
    """, unsafe_allow_html=True)

# --- LOAD CSS ---
with open(css_file) as f:
    st.markdown("<style>{}</style>".format(f.read()), unsafe_allow_html=True)


# --- CACHE DATA ---
@st.cache_data(ttl=1800)
def retrieve_list_ticket():
    list_of_symbols__ = BatchProcessManager().get_stock_list_in_database()
    if list_of_symbols__ is None or len(list_of_symbols__) < 497:
        list_of_symbols__ = ListSAndP500().tickers_list
    return list_of_symbols__


# @st.cache_data(ttl=1800)
# def batch_process(list_of_symbols__):
#     return BatchProcessManager().run_process(list_of_symbols__)


@st.cache_data(ttl=1800)
def batch_process_retrieve_data_by_stock(the_stock_in):
    return BatchProcessManager().get_stock_data_by_ticker(the_stock_in)


@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv().encode("utf-8")


@st.cache_data(ttl=1800)
def batch_process_retrieve_all_data_in_stock_table():
    return BatchProcessManager().get_all_stock_data_in_database()


PROCESS_TIME = 180  # seconds
_list_of_symbols = retrieve_list_ticket()


# --- MAIN PAGE ---
if "stock_data" not in st.session_state:
    st.session_state.stock_data = None

# --- MAIN PAGE ---
set_up_database, eda_process_tabs, deep_learning_tabs = st.tabs(
    ["Set Up Database", "EDA Data", "Deep Learning"])

# --- TABS SET UP DATABASE CONTENT ---
# with set_up_database:
#     def read_markdown_file(file_path):
#         with open(file_path, 'r', encoding='utf-8') as file:
#             data = file.read()
#         return data

#     README_PATH = './assets/Helper_Set_Up.md'
#     # --- Set Up/ Update all data in database---
#     st.markdown(read_markdown_file(README_PATH), unsafe_allow_html=True)
#     st.markdown("### B. Set Up data in database for the first time")
#     update_database = st.button("Update Database")
#     if update_database:
#         async_result = pool.apply_async(
#             batch_process, args=(_list_of_symbols,))
#         bar = st.progress(0)
#         per = PROCESS_TIME / 100
#         for i in range(100):
#             time.sleep(per)
#             bar.progress(i + 1)
#         df_dict = async_result.get()
#         st.write("Please check the data in the database")

set_up_section_control = set_up_section(_list_of_symbols, set_up_database)
set_up_section_control.run()


# --- TABS EDA DATA CONTENT ---
with eda_process_tabs:
    st.markdown("### I. Retrieve stock data symbol list")

    the_stock = st.selectbox(
        "Select the stock you want to retrieve from database (if available)", _list_of_symbols, key="the_stock")

    retrieve_col1, retrieve_col2, retrieve_col3 = st.columns(3)
    with retrieve_col1:
        btn_prepare = st.button("Retrieve stock data from database...")

    # Download data by ticket button
    with retrieve_col2:
        btn_retrieve_data_by_ticket = st.button(
            "Process File for Ticket Data in Database (csv)")

        if btn_retrieve_data_by_ticket:
            st.session_state.stock_data = the_stock
            df = batch_process_retrieve_data_by_stock(the_stock)
            if df is not None:
                df = pd.DataFrame(df)
                csv = convert_df_to_csv(df)
                st.download_button(
                    label="Download Ticket as CSV",
                    data=csv,
                    file_name=f"Ticket_{the_stock}_data.csv",
                    mime="text/csv",
                )
            else:
                st.error(
                    "No data found for this stock, please update the database first.")

    # Download all data in database button
    with retrieve_col3:
        btn_retrieve_all_data = st.button("Download All Data in Database(csv)")
        if btn_retrieve_all_data:
            st.session_state.stock_data = the_stock
            df = batch_process_retrieve_all_data_in_stock_table()
            if df is not None:
                csv = convert_df_to_csv(df)
                st.download_button(
                    label="Download All Data as CSV",
                    data=csv,
                    file_name="All_Stock_data.csv",
                    mime="text/csv",
                )
            else:
                st.error(
                    "No data found for in database, please update the database first.")

    if btn_prepare:
        st.session_state.stock_data = the_stock

    st.markdown('---')
    # --- TABS ---
    st.markdown(
        "### II. List of 500 S&P, Historical data, In Day Data, Top News, Reddit News")
    List500, Historical_data, IndayData_RealTime, news, reddit_news = st.tabs(
        ["List 500 S&P", "Historical data", "In Day Data", "Top News", "Reddit News"])

    # --- TABS LIST500 S&P CONTENT---
    with List500:
        st.write("List of 500 S&P")
        st.write(_list_of_symbols)

    # --- TABS HISTORICAL DATA CONTENT---
    with Historical_data:
        if st.session_state.stock_data is not None:
            df = batch_process_retrieve_data_by_stock(
                st.session_state.stock_data)
            if df is not None:
                df = pd.DataFrame(df)
                fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                                     open=df['open'],
                                                     high=df['high'],
                                                     low=df['low'],
                                                     close=df['close'])])
                # Add a title
                fig.update_layout(
                    title=f"{st.session_state.stock_data} Price Candlestick Chart",
                    # Center the title
                    title_x=0.3,

                    # Customize the font and size of the title
                    title_font=dict(size=24, family="Arial"),

                    # Set the background color of the plot
                    plot_bgcolor='white',

                    # Customize the grid lines
                    xaxis=dict(showgrid=True, gridwidth=1,
                               gridcolor='lightgray'),
                    yaxis=dict(showgrid=True, gridwidth=1,
                               gridcolor='lightgray'),
                )

                # Add a range slider and customize it
                fig.update_layout(
                    xaxis_rangeslider_visible=True,  # Show the range slider

                    # Customize the range slider's appearance
                    xaxis_rangeslider=dict(
                        thickness=0.1,  # Set the thickness of the slider
                        bordercolor='black',  # Set the border color
                        borderwidth=1,  # Set the border width
                    )
                )

                # Display the chart in Streamlit
                st.plotly_chart(fig)
                st.markdown(
                    f"#### Dataframe of {st.session_state.stock_data} Prices")
                st.write(df)
            else:
                st.write(
                    "No data found for this stock, please update the database first.")
        else:
            st.write("Please select the stock to retrieve the data")

# --- DEEP LEARNING TAB ---
with deep_learning_tabs:
    st.markdown("## I. Deep Learning Section")
    st.write("This section is under construction")
    st.markdown("### 1. Retrieve the data for deep learning")
    if "stock_data_deep_learning" not in st.session_state:
        st.session_state.stock_data_deep_learning = None
    the_stock_deep_learning = st.selectbox(
        "Select the stock you want to retrieve from database (if available)", _list_of_symbols, key="deep_learning")
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
        st.markdown("### 2. Data Preparation")
        phase_1_clean_data_ = phase_1_clean_data(
            [st.session_state.stock_data_deep_learning], raw_data_retrieve)
        st.markdown(
            "#### 2.1 Data Cleaning Null, Nan, '', NaN, None values inside the dataframe")
        st.write(phase_1_clean_data_.data_null_nan_cleanned)
        st.success("The number of rows total inside dataframe is: " +
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
        if "cleaning_data_method" not in st.session_state:
            st.session_state.cleaning_data_method = None

        st.markdown("#### 2.2 Data Cleaning Method")
        data_cleaning_method_deep_learning_selectbox = st.selectbox(
            "Select the method you want to process to fill the -1 values", phase_1_clean_data_.list_of_cleaning_data_method_available, key="cleaning_data_method_deep_learning")
        btn_process_data_cleaning_using_method_deep_learning = st.button(
            "Processing...")

        if btn_process_data_cleaning_using_method_deep_learning:
            st.session_state.cleaning_data_method = data_cleaning_method_deep_learning_selectbox

        if st.session_state.cleaning_data_method is not None:
            st.write(
                f"Data Cleaning Method: {st.session_state.cleaning_data_method}")
