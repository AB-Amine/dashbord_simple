import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from pymongo import MongoClient
import kafka_producer
import time

# --- Configuration ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "stock_management"

st.set_page_config(page_title="Real-Time Analytics", layout="wide")

# --- Database Connection ---
@st.cache_resource
def get_mongo_client():
    """Establishes a connection to MongoDB."""
    return MongoClient(MONGO_URI)

def get_mongo_data(collection_name):
    """Fetches data from a MongoDB collection and returns as a DataFrame."""
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[collection_name]
    data = list(collection.find({}, {'_id': 0})) # Exclude the default _id field
    return pd.DataFrame(data) if data else pd.DataFrame()

# --- Main Application ---
def main():
    with st.sidebar:
        selected = option_menu(
            menu_title="Main Menu",
            options=["Accountant", "Administrator"],
            icons=["person-circle", "shield-lock"],
            menu_icon="cast",
            default_index=0,
        )

    st.title("Real-Time Economic Analytics Platform")

    if selected == "Accountant":
        st.header("Data Ingestion Portal")
        st.markdown("""
            Upload the Excel file containing Sales, Inventory, and Client data. 
            The system will process the file and feed the data into the real-time analytics pipeline.
        """)
        
        uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")
        
        if uploaded_file is not None:
            with st.spinner('Processing file and sending data to the pipeline...'):
                # The kafka_producer.main can now accept a file-like object
                kafka_producer.main(uploaded_file)
                time.sleep(2) # Give a moment for visual feedback
            st.success("File processed and data sent to the analytics pipeline successfully!")
            st.info("Switch to the 'Administrator' view to see the results.")

    elif selected == "Administrator":
        st.header("Real-Time Analytics Dashboard")

        if st.button('Refresh Data'):
            st.cache_data.clear()
            st.experimental_rerun()

        tab1, tab2, tab3 = st.tabs(["🏆 Product Winners", "⚠️ Loss-Risk Products", "💸 Client Promotions"])
        
        with tab1:
            st.subheader("Top 10 Most Profitable Products")
            product_winners_df = get_mongo_data("product_winners")
            if not product_winners_df.empty:
                st.dataframe(product_winners_df, use_container_width=True)
                st.bar_chart(product_winners_df.set_index('name')['total_profit'])
            else:
                st.info("Awaiting data... No product winner analytics available yet.")

        with tab2:
            st.subheader("Products at Risk of Loss (Expiring or Dead Stock)")
            loss_products_df = get_mongo_data("loss_risk_products")
            if not loss_products_df.empty:
                st.dataframe(loss_products_df, use_container_width=True)
            else:
                st.info("Awaiting data... No loss-risk products detected.")
        
        with tab3:
            st.subheader("Client Loyalty & Promotion Eligibility")
            clients_df = get_mongo_data("clients")
            if not clients_df.empty:
                # Highlight clients eligible for promotion
                def highlight_promo(row):
                    return ['background-color: #d4edda'] * len(row) if row.get("eligible_promo") else [''] * len(row)
                
                st.dataframe(clients_df.style.apply(highlight_promo, axis=1), use_container_width=True)
            else:
                st.info("Awaiting data... No client information available yet.")


if __name__ == '__main__':
    main()