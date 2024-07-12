import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from BatchProcess.BatchProcess import BatchProcessManager


@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv().encode("utf-8")


@st.cache_data(ttl=1800)
def batch_process_retrieve_all_data_in_stock_table():
    return BatchProcessManager().get_all_stock_data_in_database()


@st.cache_data(ttl=1800)
def batch_process_retrieve_data_by_stock(the_stock_in):
    return BatchProcessManager().get_stock_data_by_ticker(the_stock_in)


class exploring_data_analysis_section():
    def __init__(self, _list_of_symbols, eda_process_tab):
        self.eda_process_tabs = eda_process_tab
        self.list_of_symbols = _list_of_symbols

    def run(self):
        with self.eda_process_tabs:
            st.markdown("### I. Retrieve stock data symbol list")

            the_stock = st.selectbox(
                "Select the stock you want to retrieve from database (if available)", self.list_of_symbols, key="the_stock")
            btn_prepare = self.download_button_section(the_stock)
            if btn_prepare:
                st.session_state.stock_data = the_stock

            st.markdown('---')
            # --- TABS ---
            st.markdown(
                "### II. List of 500 S&P, Historical data, In Day Data, Top News, Reddit News")
            list500_tab, historical_data_tab, indaydata_realtime, news, reddit_news = st.tabs(
                ["List 500 S&P", "Historical data", "In Day Data", "Top News", "Reddit News"])

            # --- TABS LIST500 S&P CONTENT---
            self.list_of_500_content_section(list500_tab)

            # --- TABS HISTORICAL DATA CONTENT---
            self.historical_content_section(historical_data_tab)

    def download_button_section(self, the_stock):
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
            btn_retrieve_all_data = st.button(
                "Download All Data in Database(csv)")
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

        return btn_prepare

    def list_of_500_content_section(self, list500_tab):
        with list500_tab:
            st.write("List of 500 S&P")
            st.write(self.list_of_symbols)

    def historical_content_section(self, historical_data_tab):
        with historical_data_tab:
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
