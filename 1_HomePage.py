from BatchProcess.DataSource.ListSnP500.ListSnP500Collect import ListSAndP500
from DeepLearningProcess.Phase1CleanData import phase_1_clean_data
from UI.Deep_Leaning_Section import deep_learning_model_section
from UI.EDA_Section import exploring_data_analysis_section
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

# --- SET UP DATABASE SECTION ---
set_up_section_control = set_up_section(_list_of_symbols, set_up_database)
set_up_section_control.run()

# --- TABS EDA DATA SECTION ---
eda_process_control = exploring_data_analysis_section(
    _list_of_symbols, eda_process_tabs)
eda_process_control.run()

# --- DEEP LEARNING SECTION ---
deep_learning_model_control = deep_learning_model_section(
    _list_of_symbols, deep_learning_tabs)
deep_learning_model_control.run()
