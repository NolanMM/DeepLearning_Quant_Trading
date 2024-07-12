from multiprocessing.pool import ThreadPool
from BatchProcess.BatchProcess import BatchProcessManager
import streamlit as st
import time
pool = ThreadPool(processes=6)
PROCESS_TIME = 180  # seconds
README_PATH = './assets/Helper_Set_Up.md'


@st.cache_data(ttl=1800)
def batch_process(list_of_symbols__):
    return BatchProcessManager().run_process(list_of_symbols__)


@st.cache_data(ttl=1800)
def read_markdown_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = file.read()
    return data


class set_up_section():
    def __init__(self, list_of_symbols, set_up_database):
        self.list_of_symbols = list_of_symbols
        self.set_up_database_tab = set_up_database

    def run(self):
        # --- TABS SET UP DATABASE CONTENT ---
        with self.set_up_database_tab:
            # --- Set Up/ Update all data in database---
            st.markdown(read_markdown_file(README_PATH),
                        unsafe_allow_html=True)
            st.markdown("### B. Set Up data in database for the first time")
            update_database = st.button(
                "Update Database..\nPlease wait for next message..")
            if update_database:
                async_result = pool.apply_async(
                    batch_process, args=(self.list_of_symbols,))
                bar = st.progress(0)
                per = PROCESS_TIME / 100
                for i in range(100):
                    time.sleep(per)
                    bar.progress(i + 1)
                df_dict = async_result.get()
                st.write("Please check the data in the database")
