#!/usr/bin/env python3
"""
User Interface class for the Real-Time Economic Analytics Platform.
Handles both Accountant and Administrator views.
"""

import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from pymongo import MongoClient
import time
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

# Configure logging for Admin Interface
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AdminInterface")


class UserInterface:
    """
    Class responsible for the user interface of the application.
    """
    
    def __init__(self, config: Config):
        """Initialize UserInterface with configuration."""
        self.config = config
        self.mongo_client = None
        self.mongodb_accountant_manager = MongoDBAccountantManager(config)
    
    def get_mongo_client(self):
        """Get MongoDB client with caching."""
        if not self.mongo_client:
            self.mongo_client = MongoClient(self.config.MONGO_URI)
        return self.mongo_client
    
    def get_mongo_data(self, collection_name: str) -> pd.DataFrame:
        """Fetches data from a MongoDB collection and returns as a DataFrame.
        
        This is the ONLY way Admin Interface gets data - reads from MongoDB only.
        Never triggers Spark or Kafka - completely decoupled.
        """
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            collection = db[collection_name]
            data = list(collection.find({}, {'_id': 0}))  # Exclude the default _id field
            
            if data:
                logger.info(f"📊 Admin Interface: Successfully read {len(data)} records from MongoDB collection '{collection_name}'")
                return pd.DataFrame(data)
            else:
                logger.info(f"📉 Admin Interface: No data found in MongoDB collection '{collection_name}' - showing empty results")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to get MongoDB data from '{collection_name}': {e}")
            return pd.DataFrame()
    
    def run(self):
        """Run the main user interface.
        
        This method handles Streamlit reruns gracefully and ensures
        the UI only reads from MongoDB without triggering backend processes.
        """
        logger.info("🎯 User Interface: Starting Streamlit application")
        
        # Initialize session state for page config tracking
        if 'page_config_set' not in st.session_state:
            st.session_state.page_config_set = False
        
        # Set page config only once (Streamlit requirement)
        if not st.session_state.page_config_set:
            st.set_page_config(page_title="Real-Time Analytics", layout="wide")
            st.session_state.page_config_set = True
            logger.info("📱 User Interface: Page configuration set")
        
        # Add refresh button for manual refresh (optional)
        if st.sidebar.button('🔄 Refresh Data'):
            logger.info("🔄 User Interface: Manual refresh requested")
            st.cache_data.clear()
            st.experimental_rerun()
        
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
            logger.info("👩‍💼 User Interface: Accountant view selected")
            self.show_accountant_view()
        elif selected == "Administrator":
            logger.info("👨‍💼 User Interface: Administrator view selected")
            self.show_administrator_view()
    
    def show_accountant_view(self):
        """Show the Accountant view for manual data entry."""
        st.header("Accountant Data Entry Portal")
        st.markdown("""
            **Manual Data Entry Interface**
            Use the tabs below to manually enter Products, Stock, Clients, and Sales data.
            All data is automatically persisted in the SQLite database and fed into the real-time analytics pipeline.
        """)
        
        # Create navigation tabs for different data entry forms
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📦 Add Product", 
            "📊 Add Stock", 
            "👥 Add Client",
            "💰 Add Sale",
            "📋 View Data"
        ])
        
        with tab1:
            self._show_add_product_tab()
        
        with tab2:
            self._show_add_stock_tab()
        
        with tab3:
            self._show_add_client_tab()
        
        with tab4:
            self._show_add_sale_tab()
        
        with tab5:
            self._show_view_data_tab()
        
        # Add MongoDB export tab
        with tab6:
            self._show_mongodb_export_tab()
    
    def _show_add_product_tab(self):
        """Show the Add Product form."""
        st.subheader("Add New Product")
        
        with st.form("add_product_form"):
            name = st.text_input("Product Name", placeholder="e.g., Organic Apples")
            category = st.text_input("Category", placeholder="e.g., Produce")
            
            col1, col2 = st.columns(2)
            with col1:
                buy_price = st.number_input("Buy Price (€)", min_value=0.01, step=0.01, format="%.2f")
            with col2:
                sell_price = st.number_input("Sell Price (€)", min_value=0.01, step=0.01, format="%.2f")
            
            min_margin_threshold = st.slider("Minimum Margin Threshold (%)", 0, 100, 20)
            
            submit_button = st.form_submit_button("Save Product", type="primary")
            
            if submit_button:
                if not name or not category:
                    st.error("❌ Name and category are required!")
                elif buy_price <= 0 or sell_price <= 0:
                    st.error("❌ Prices must be positive values!")
                else:
                    success, message = self.mongodb_accountant_manager.add_product(
                        name=name,
                        category=category,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        min_margin_threshold=min_margin_threshold
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
    
    def _show_add_stock_tab(self):
        """Show the Add Stock form."""
        st.subheader("Add Stock Inventory")
        
        # Get available products for selection
        products = self.mongodb_accountant_manager.get_all_products()
        
        if not products:
            st.warning("⚠️ No products available. Please add products first.")
            return
        
        with st.form("add_stock_form"):
            product_options = {f"{p['name']} ({p['category']})": p['product_id'] for p in products}
            selected_product = st.selectbox("Select Product", options=list(product_options.keys()))
            product_id = product_options[selected_product]
            
            col1, col2 = st.columns(2)
            with col1:
                quantity = st.number_input("Quantity", min_value=1, step=1)
            with col2:
                batch_no = st.text_input("Batch Number", placeholder="e.g., BATCH-2024-001")
            
            expiry_date = st.date_input("Expiry Date")
            
            submit_button = st.form_submit_button("Save Stock", type="primary")
            
            if submit_button:
                if not batch_no:
                    st.error("❌ Batch number is required!")
                else:
                    expiry_date_str = expiry_date.strftime('%Y-%m-%d')
                    success, message = self.mongodb_accountant_manager.add_stock(
                        product_id=product_id,
                        quantity=quantity,
                        batch_no=batch_no,
                        expiry_date=expiry_date_str
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
    
    def _show_add_client_tab(self):
        """Show the Add Client form."""
        st.subheader("Add New Client")
        
        with st.form("add_client_form"):
            name = st.text_input("Client Name", placeholder="e.g., John Doe")
            email = st.text_input("Email (optional)", placeholder="e.g., john@example.com")
            phone = st.text_input("Phone (optional)", placeholder="e.g., +1234567890")
            
            submit_button = st.form_submit_button("Save Client", type="primary")
            
            if submit_button:
                if not name:
                    st.error("❌ Client name is required!")
                else:
                    success, message = self.mongodb_accountant_manager.add_client(
                        name=name,
                        email=email if email else None,
                        phone=phone if phone else None
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                    else:
                        st.error(f"❌ {message}")
    
    def _show_add_sale_tab(self):
        """Show the Add Sale form."""
        st.subheader("Record New Sale")
        
        # Get available clients and products
        clients = self.mongodb_accountant_manager.get_all_clients()
        products = self.mongodb_accountant_manager.get_all_products()
        
        if not clients:
            st.warning("⚠️ No clients available. Please add clients first.")
            return
        
        if not products:
            st.warning("⚠️ No products available. Please add products first.")
            return
        
        with st.form("add_sale_form"):
            # Client selection
            client_options = {c['name']: c['client_id'] for c in clients}
            selected_client = st.selectbox("Select Client", options=list(client_options.keys()))
            client_id = client_options[selected_client]
            
            st.markdown("---")
            st.write("**Sale Items**")
            
            # Dynamic sale items
            num_items = st.number_input("Number of Items", min_value=1, max_value=10, value=1)
            
            items = []
            for i in range(num_items):
                st.markdown(f"**Item {i+1}**")
                col1, col2 = st.columns(2)
                
                with col1:
                    product_options = {f"{p['name']} (€{p['sell_price']:.2f})": p['product_id'] for p in products}
                    selected_product = st.selectbox(
                        f"Product {i+1}", 
                        options=list(product_options.keys()),
                        key=f"product_{i}"
                    )
                    product_id = product_options[selected_product]
                
                with col2:
                    quantity = st.number_input(
                        f"Quantity", 
                        min_value=1, 
                        step=1, 
                        key=f"quantity_{i}"
                    )
                
                items.append({
                    'product_id': product_id,
                    'quantity': quantity
                })
                st.markdown("---")
            
            submit_button = st.form_submit_button("Record Sale", type="primary")
            
            if submit_button:
                success, message = self.mongodb_accountant_manager.add_sale(
                    client_id=client_id,
                    items=items
                )
                
                if success:
                    st.success(f"✅ {message}")
                    st.balloons()
                else:
                    st.error(f"❌ {message}")
    
    def _show_view_data_tab(self):
        """Show the View Data tab with all recorded data."""
        st.subheader("View All Data")
        
        st.info("💡 **Tip**: After adding data, click the refresh button or switch tabs to see updates.")
        
        # Add refresh button for manual refresh
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            st.write("")  # Spacer
        with col2:
            if st.button("🔄 Refresh Data", key="refresh_view_data"):
                st.cache_data.clear()
                st.experimental_rerun()
        with col3:
            st.write("")  # Spacer
        
        st.markdown("---")
        
        # Create sub-tabs for different data types
        data_tab1, data_tab2, data_tab3, data_tab4 = st.tabs([
            "📦 Products", "📊 Stock", "👥 Clients", "💰 Sales"
        ])
        
        with data_tab1:
            self._show_products_data()
        
        with data_tab2:
            self._show_stock_data()
        
        with data_tab3:
            self._show_clients_data()
        
        with data_tab4:
            self._show_sales_data()
    
    def _show_products_data(self):
        """Display products data in a table."""
        products = self.mongodb_accountant_manager.get_all_products()
        
        if products:
            products_df = pd.DataFrame(products)
            products_df = products_df[['product_id', 'name', 'category', 'buy_price', 'sell_price', 'min_margin_threshold', 'created_at']]
            
            st.dataframe(
                products_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Show summary statistics
            st.markdown("**Summary Statistics**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Products", len(products))
            with col2:
                avg_margin = products_df['min_margin_threshold'].mean()
                st.metric("Avg Margin Threshold", f"{avg_margin:.1f}%")
            with col3:
                total_value = (products_df['buy_price'] * 100).sum()  # Assuming avg stock of 100 units
                st.metric("Estimated Inventory Value", f"€{total_value:,.2f}")
        else:
            st.info("📊 No products found yet. Add products using the '📦 Add Product' tab to see them here.")
    
    def _show_stock_data(self):
        """Display stock data in a table."""
        stock_items = self.mongodb_accountant_manager.get_all_stock()
        
        if stock_items:
            stock_df = pd.DataFrame(stock_items)
            stock_df = stock_df[['stock_id', 'product_name', 'batch_no', 'quantity', 'expiry_date', 'last_movement']]
            
            # Convert expiry_date to datetime for sorting
            stock_df['expiry_date'] = pd.to_datetime(stock_df['expiry_date'])
            stock_df = stock_df.sort_values('expiry_date')
            
            st.dataframe(
                stock_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Show summary statistics
            st.markdown("**Summary Statistics**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Stock Items", len(stock_items))
            with col2:
                total_quantity = stock_df['quantity'].sum()
                st.metric("Total Units in Stock", total_quantity)
            with col3:
                # Find items expiring soon (within 30 days)
                expiring_soon = stock_df[stock_df['expiry_date'] <= pd.Timestamp.now() + pd.Timedelta(days=30)]
                st.metric("Expiring Soon", len(expiring_soon))
        else:
            st.info("📊 No stock items found yet. Add stock using the '📊 Add Stock' tab to see inventory here.")
    
    def _show_clients_data(self):
        """Display clients data in a table."""
        clients = self.mongodb_accountant_manager.get_all_clients()
        
        if clients:
            clients_df = pd.DataFrame(clients)
            clients_df = clients_df[['client_id', 'name', 'email', 'phone', 'total_revenue_generated', 'loyalty_tier', 'created_at']]
            
            st.dataframe(
                clients_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Show summary statistics
            st.markdown("**Summary Statistics**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Clients", len(clients))
            with col2:
                total_revenue = clients_df['total_revenue_generated'].sum()
                st.metric("Total Revenue Generated", f"€{total_revenue:,.2f}")
            with col3:
                gold_clients = len(clients_df[clients_df['loyalty_tier'] == 'Gold'])
                st.metric("Gold Tier Clients", gold_clients)
        else:
            st.info("📊 No clients found yet. Add clients using the '👥 Add Client' tab to see them here.")
    
    def _show_sales_data(self):
        """Display sales data in a table."""
        sales = self.mongodb_accountant_manager.get_all_sales()
        
        if sales:
            sales_df = pd.DataFrame(sales)
            sales_df = sales_df[['invoice_id', 'client_name', 'timestamp', 'total_amount']]
            sales_df['timestamp'] = pd.to_datetime(sales_df['timestamp'])
            sales_df = sales_df.sort_values('timestamp', ascending=False)
            
            st.dataframe(
                sales_df,
                use_container_width=True,
                hide_index=True
            )
            
            # Show summary statistics
            st.markdown("**Summary Statistics**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Sales", len(sales))
            with col2:
                total_revenue = sales_df['total_amount'].sum()
                st.metric("Total Sales Revenue", f"€{total_revenue:,.2f}")
            with col3:
                if len(sales) > 0:
                    avg_sale = sales_df['total_amount'].mean()
                    st.metric("Average Sale Amount", f"€{avg_sale:.2f}")
        else:
            st.info("📊 No sales found yet. Record sales using the '💰 Add Sale' tab to see them here.")
    
    def _show_mongodb_export_tab(self):
        """Show the MongoDB Export tab for data lake functionality."""
        st.subheader("🗄️ MongoDB Data Lake Export")
        logger.info("📊 Admin Interface: Loading MongoDB export tab")
        
        st.markdown("""
        **Data Lake Export Functionality**
        Export MongoDB collections to Excel files for historical archiving and analytics.
        All exports are saved to the data lake directory with timestamps.
        """)
        
        # Create export buttons for each collection
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button('📥 Export Products to Excel'):
                logger.info("📊 Admin Interface: Exporting products to Excel")
                success, msg = self.mongodb_accountant_manager.export_to_data_lake('products')
                if success:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
            
            if st.button('📥 Export Clients to Excel'):
                logger.info("📊 Admin Interface: Exporting clients to Excel")
                success, msg = self.mongodb_accountant_manager.export_to_data_lake('clients')
                if success:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
        
        with col2:
            if st.button('📥 Export Sales to Excel'):
                logger.info("📊 Admin Interface: Exporting sales to Excel")
                success, msg = self.mongodb_accountant_manager.export_to_data_lake('sales')
                if success:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
            
            if st.button('📥 Export Inventory to Excel'):
                logger.info("📊 Admin Interface: Exporting inventory to Excel")
                success, msg = self.mongodb_accountant_manager.export_to_data_lake('inventory')
                if success:
                    st.success(f"✅ {msg}")
                else:
                    st.error(f"❌ {msg}")
        
        # Add bulk export option
        st.markdown("---")
        if st.button('📦 Export All Collections to Data Lake'):
            logger.info("📊 Admin Interface: Exporting all collections to data lake")
            
            collections = ['products', 'clients', 'sales', 'inventory']
            all_success = True
            
            for collection in collections:
                success, msg = self.mongodb_accountant_manager.export_to_data_lake(collection)
                if success:
                    st.success(f"✅ {collection}: {msg}")
                else:
                    st.error(f"❌ {collection}: {msg}")
                    all_success = False
            
            if all_success:
                st.balloons()
                st.success("🎉 All collections exported successfully to data lake!")
            else:
                st.error("⚠️  Some collections failed to export")
        
        # Show data lake info
        st.markdown("---")
        st.info("💡 **Data Lake Information**")
        st.write("""
        - **Location**: `/tmp/data_lake` (configurable)
        - **Format**: Excel (.xlsx) with timestamps
        - **Purpose**: Historical archiving, compliance, offline analytics
        - **Retention**: Files are preserved until manually deleted
        """)
    
    def show_administrator_view(self):
        """Show the Administrator view for analytics dashboard.
        
        This view reads ONLY from MongoDB and displays the last known analytics state.
        It never triggers Spark or Kafka processing.
        """
        st.header("📊 Real-Time Analytics Dashboard")
        logger.info("📊 Admin Interface: Loading analytics dashboard")
        
        # System Status Section
        st.subheader("🔧 System Status")
        
        # Check MongoDB connection status
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            
            # Count documents in each collection
            product_winners_count = db.product_winners.count_documents({})
            loss_risk_count = db.loss_risk_products.count_documents({})
            clients_count = db.clients.count_documents({})
            inventory_count = db.inventory.count_documents({})
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🏆 Product Winners", product_winners_count)
            with col2:
                st.metric("⚠️ Loss Risk Products", loss_risk_count)
            with col3:
                st.metric("👥 Clients", clients_count)
            with col4:
                st.metric("📦 Inventory", inventory_count)
                
            logger.info(f"📊 Admin Interface: System status - Products: {product_winners_count}, Loss Risk: {loss_risk_count}, Clients: {clients_count}, Inventory: {inventory_count}")
            
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to get system status: {e}")
            st.error("❌ Unable to connect to MongoDB. Please ensure MongoDB is running.")
            return
        
        # Add info about real-time system
        st.info("📈 **Real-Time System Info:** Spark Streaming runs independently. This interface reads from MongoDB only.")
        
        # Add historical data re-ingestion button
        col1, col2 = st.columns(2)
        with col1:
            if st.button('🔄 Refresh Data'):
                logger.info("🔄 Admin Interface: Manual refresh requested")
                st.cache_data.clear()
                st.experimental_rerun()
        
        with col2:
            if st.button('🎯 Reprocess Historical Data'):
                logger.info("🎯 Admin Interface: Historical data re-ingestion requested")
                st.info("🚀 Starting historical data re-ingestion... This may take a moment.")
                
                # Trigger historical re-ingestion
                try:
                    from main.application import RealTimeAnalyticsApplication
                    app = RealTimeAnalyticsApplication()
                    success = app.trigger_historical_reingestion()
                    
                    if success:
                        st.success("✅ Historical data re-ingestion started successfully!")
                        st.info("📊 Spark will process the data and update MongoDB automatically.")
                        st.info("🔄 Refresh this page in a few moments to see the updated analytics.")
                    else:
                        st.error("❌ Failed to start historical data re-ingestion.")
                except Exception as e:
                    logger.error(f"❌ Admin Interface: Failed to trigger re-ingestion: {e}")
                    st.error(f"❌ Error starting re-ingestion: {e}")
        
        # Enhanced MongoDB Collections Display
        st.subheader("📊 All MongoDB Collections")
        logger.info("📊 Admin Interface: Loading all MongoDB collections")
        
        # Get all collections from MongoDB
        try:
            all_collections = db.list_collection_names()
            logger.info(f"📊 Admin Interface: Found {len(all_collections)} collections: {all_collections}")
            
            # Filter out system collections
            analytics_collections = [col for col in all_collections if not col.startswith('system.')]
            
            # Create tabs for each collection
            collection_tabs = st.tabs([f"📋 {col}" for col in analytics_collections])
            
            for i, collection_name in enumerate(analytics_collections):
                with collection_tabs[i]:
                    self._show_mongodb_collection(collection_name)
                    
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to list MongoDB collections: {e}")
            st.error(f"❌ Error loading MongoDB collections: {e}")
        
        # Advanced Analytics Section
        st.subheader("📈 Advanced Analytics")
        
        # Create analytics tabs
        analytics_tab1, analytics_tab2, analytics_tab3, analytics_tab4 = st.tabs([
            "🏆 Product Performance", 
            "⚠️ Risk Analysis", 
            "💸 Revenue Analysis",
            "📊 Comprehensive Overview"
        ])
        
        with analytics_tab1:
            self._show_product_performance_analytics()
        
        with analytics_tab2:
            self._show_risk_analysis()
        
        with analytics_tab3:
            self._show_revenue_analysis()
        
        with analytics_tab4:
            self._show_comprehensive_overview()
    
    def show_product_winners_tab(self):
        """Show the Product Winners tab.
        
        This reads ONLY from MongoDB - completely decoupled from Spark.
        Shows last known results even if no new data arrives.
        """
        st.subheader("🏆 Top 10 Most Profitable Products")
        logger.info("📊 Admin Interface: Loading product winners from MongoDB...")
        
        product_winners_df = self.get_mongo_data("product_winners")
        
        if not product_winners_df.empty:
            logger.info(f"📈 Admin Interface: Displaying {len(product_winners_df)} product winners")
            st.dataframe(product_winners_df, use_container_width=True)
            st.bar_chart(product_winners_df.set_index('name')['total_profit'])
        else:
            logger.info("📉 Admin Interface: No product winners data available - showing placeholder")
            st.info("📊 Awaiting real-time data... No product winner analytics available yet.")
    
    def show_loss_risk_products_tab(self):
        """Show the Loss-Risk Products tab.
        
        This reads ONLY from MongoDB - completely decoupled from Spark.
        Shows last known results even if no new data arrives.
        """
        st.subheader("⚠️ Products at Risk of Loss (Expiring or Dead Stock)")
        logger.info("📊 Admin Interface: Loading loss risk products from MongoDB...")
        
        loss_products_df = self.get_mongo_data("loss_risk_products")
        
        if not loss_products_df.empty:
            logger.info(f"📈 Admin Interface: Displaying {len(loss_products_df)} loss risk products")
            st.dataframe(loss_products_df, use_container_width=True)
        else:
            logger.info("📉 Admin Interface: No loss risk products data available - showing placeholder")
            st.info("📊 Awaiting real-time data... No loss-risk products detected.")
    
    def _show_mongodb_collection(self, collection_name: str):
        """Display a MongoDB collection with comprehensive analytics."""
        logger.info(f"📊 Admin Interface: Loading collection '{collection_name}'")
        
        try:
            # Fetch data from MongoDB
            df = self.get_mongo_data(collection_name)
            
            if not df.empty:
                logger.info(f"📈 Admin Interface: Displaying {len(df)} records from '{collection_name}'")
                
                # Display collection title
                st.subheader(f"📋 {collection_name}")
                
                # Display data table
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Show comprehensive statistics
                with st.expander("📊 Detailed Statistics"):
                    st.write(f"**Total Records:** {len(df)}")
                    if len(df) > 0:
                        st.write(f"**Columns:** {', '.join(df.columns.tolist())}")
                        
                        # Show basic statistics for all columns
                        st.write("**Data Types:**")
                        dtypes_df = pd.DataFrame(df.dtypes, columns=['Data Type'])
                        st.dataframe(dtypes_df, use_container_width=True)
                        
                        # Show statistics for numeric columns
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        if len(numeric_cols) > 0:
                            st.write("**Numeric Statistics:**")
                            st.dataframe(df[numeric_cols].describe(), use_container_width=True)
                            
                            # Show distribution charts for numeric columns
                            st.write("**Distribution Charts:**")
                            for col in numeric_cols:
                                st.line_chart(df[col])
                        
                        # Show value counts for categorical columns
                        categorical_cols = df.select_dtypes(include=['object']).columns
                        if len(categorical_cols) > 0:
                            st.write("**Categorical Analysis:**")
                            for col in categorical_cols:
                                if len(df[col].unique()) <= 20:  # Only show if reasonable number of unique values
                                    value_counts = df[col].value_counts()
                                    st.write(f"**{col} distribution:**")
                                    st.bar_chart(value_counts)
                
                # Export options
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"{collection_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                json_data = df.to_json(orient='records', indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_data,
                    file_name=f"{collection_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
                
            else:
                logger.info(f"📉 Admin Interface: No data found in collection '{collection_name}' - showing placeholder")
                st.info(f"📊 No data available in collection '{collection_name}'. This collection will be populated when Spark processes relevant events.")
                
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to load collection '{collection_name}': {e}")
            st.error(f"❌ Error loading collection '{collection_name}': {e}")
    
    def _show_product_performance_analytics(self):
        """Show comprehensive product performance analytics."""
        st.subheader("🏆 Product Performance Analytics")
        logger.info("📊 Admin Interface: Loading product performance analytics")
        
        try:
            # Get product winners data
            product_winners_df = self.get_mongo_data("product_winners")
            inventory_df = self.get_mongo_data("inventory")
            
            if not product_winners_df.empty:
                # Show top performers
                st.write("### 🥇 Top Performing Products")
                st.dataframe(product_winners_df.head(10), use_container_width=True)
                
                # Create visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("#### 📊 Profit Distribution")
                    st.bar_chart(product_winners_df.set_index('name')['total_profit'])
                
                with col2:
                    st.write("#### 📈 Top 5 Products")
                    top_5 = product_winners_df.head(5)
                    st.bar_chart(top_5.set_index('name')['total_profit'])
                
                # Show detailed metrics
                if len(product_winners_df) > 0:
                    total_profit = product_winners_df['total_profit'].sum()
                    avg_profit = product_winners_df['total_profit'].mean()
                    max_profit = product_winners_df['total_profit'].max()
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("💰 Total Profit", f"€{total_profit:,.2f}")
                    with col2:
                        st.metric("📊 Avg Profit", f"€{avg_profit:,.2f}")
                    with col3:
                        st.metric("🔝 Max Profit", f"€{max_profit:,.2f}")
                    with col4:
                        st.metric("🏆 Top Products", len(product_winners_df))
            else:
                st.info("📊 No product performance data available yet. Waiting for Spark processing...")
                
            # Show inventory analytics if available
            if not inventory_df.empty:
                st.write("### 📦 Inventory Analysis")
                
                # Show inventory by category
                if 'category' in inventory_df.columns:
                    category_counts = inventory_df['category'].value_counts()
                    st.write("#### 📊 Products by Category")
                    st.bar_chart(category_counts)
                
                # Show quantity distribution
                if 'quantity' in inventory_df.columns:
                    st.write("#### 📈 Quantity Distribution")
                    st.line_chart(inventory_df['quantity'])
                    
                    # Show low stock alert
                    low_stock = inventory_df[inventory_df['quantity'] < 10]
                    if not low_stock.empty:
                        st.warning(f"⚠️ {len(low_stock)} products have low stock (< 10 units)")
                        st.dataframe(low_stock[['name', 'quantity']], use_container_width=True)
            
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to load product performance analytics: {e}")
            st.error(f"❌ Error loading product performance analytics: {e}")
    
    def _show_risk_analysis(self):
        """Show comprehensive risk analysis."""
        st.subheader("⚠️ Risk Analysis Dashboard")
        logger.info("📊 Admin Interface: Loading risk analysis")
        
        try:
            # Get risk data
            loss_risk_df = self.get_mongo_data("loss_risk_products")
            inventory_df = self.get_mongo_data("inventory")
            
            if not loss_risk_df.empty:
                st.write("### 🚨 Products at Risk")
                st.dataframe(loss_risk_df, use_container_width=True)
                
                # Risk metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("⚠️ Total Risk Products", len(loss_risk_df))
                with col2:
                    if 'expiry_date' in loss_risk_df.columns:
                        expiring_soon = loss_risk_df[loss_risk_df['expiry_date'].notna()]
                        st.metric("🗓️ Expiring Soon", len(expiring_soon))
                with col3:
                    if 'last_movement' in loss_risk_df.columns:
                        dead_stock = loss_risk_df[loss_risk_df['last_movement'].notna()]
                        st.metric("💀 Dead Stock", len(dead_stock))
                
                # Visualizations
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'expiry_date' in loss_risk_df.columns:
                        st.write("#### 🗓️ Expiry Date Distribution")
                        # Convert to datetime if needed
                        expiry_dates = pd.to_datetime(loss_risk_df['expiry_date'], errors='coerce')
                        if expiry_dates.notna().any():
                            st.line_chart(expiry_dates.value_counts().sort_index())
                
                with col2:
                    if 'category' in loss_risk_df.columns:
                        st.write("#### 📊 Risk by Category")
                        st.bar_chart(loss_risk_df['category'].value_counts())
            else:
                st.success("🎉 No products at risk detected!")
            
            # Show inventory risk analysis
            if not inventory_df.empty:
                st.write("### 📊 Inventory Risk Indicators")
                
                # Calculate risk indicators
                if 'expiry_date' in inventory_df.columns:
                    # Convert to datetime
                    inventory_df['expiry_date_dt'] = pd.to_datetime(inventory_df['expiry_date'], errors='coerce')
                    
                    # Find products expiring within 30 days
                    today = pd.Timestamp.now()
                    expiring_30_days = inventory_df[
                        (inventory_df['expiry_date_dt'] > today) & 
                        (inventory_df['expiry_date_dt'] <= today + pd.Timedelta(days=30))
                    ]
                    
                    if not expiring_30_days.empty:
                        st.warning(f"⚠️ {len(expiring_30_days)} products expiring within 30 days")
                        st.dataframe(expiring_30_days[['name', 'expiry_date', 'quantity']], use_container_width=True)
                
                # Find products with no movement
                if 'last_movement' in inventory_df.columns:
                    inventory_df['last_movement_dt'] = pd.to_datetime(inventory_df['last_movement'], errors='coerce')
                    dead_stock_30 = inventory_df[
                        inventory_df['last_movement_dt'] < today - pd.Timedelta(days=30)
                    ]
                    
                    if not dead_stock_30.empty:
                        st.warning(f"💀 {len(dead_stock_30)} products with no movement in 30+ days")
                        st.dataframe(dead_stock_30[['name', 'last_movement', 'quantity']], use_container_width=True)
            
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to load risk analysis: {e}")
            st.error(f"❌ Error loading risk analysis: {e}")
    
    def _show_revenue_analysis(self):
        """Show comprehensive revenue analysis."""
        st.subheader("💸 Revenue Analysis Dashboard")
        logger.info("📊 Admin Interface: Loading revenue analysis")
        
        try:
            # Get client and revenue data
            clients_df = self.get_mongo_data("clients")
            
            if not clients_df.empty:
                # Revenue metrics
                total_revenue = clients_df['total_revenue_generated'].sum()
                avg_revenue = clients_df['total_revenue_generated'].mean()
                max_revenue = clients_df['total_revenue_generated'].max()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("💰 Total Revenue", f"€{total_revenue:,.2f}")
                with col2:
                    st.metric("📊 Avg Revenue/Client", f"€{avg_revenue:,.2f}")
                with col3:
                    st.metric("🔝 Max Revenue", f"€{max_revenue:,.2f}")
                with col4:
                    st.metric("👥 Total Clients", len(clients_df))
                
                # Client segmentation
                st.write("### 👥 Client Segmentation")
                
                # Define revenue tiers
                clients_df['revenue_tier'] = pd.cut(
                    clients_df['total_revenue_generated'],
                    bins=[0, 1000, 5000, 10000, 50000, float('inf')],
                    labels=['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
                )
                
                tier_counts = clients_df['revenue_tier'].value_counts()
                st.bar_chart(tier_counts)
                
                # Show top clients
                st.write("### 🏆 Top Clients by Revenue")
                top_clients = clients_df.sort_values('total_revenue_generated', ascending=False).head(10)
                st.dataframe(top_clients[['name', 'total_revenue_generated', 'loyalty_tier']], use_container_width=True)
                
                # Revenue distribution chart
                st.write("### 📊 Revenue Distribution")
                st.line_chart(clients_df['total_revenue_generated'])
                
                # Promotion eligibility
                if 'eligible_promo' in clients_df.columns:
                    promo_eligible = clients_df[clients_df['eligible_promo'] == True]
                    st.write(f"### 🎁 Promotion Eligibility")
                    st.metric("🎟️ Eligible Clients", len(promo_eligible))
                    
                    if not promo_eligible.empty:
                        st.dataframe(promo_eligible[['name', 'total_revenue_generated', 'loyalty_tier']], use_container_width=True)
            else:
                st.info("📊 No revenue data available yet. Waiting for client transactions...")
                
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to load revenue analysis: {e}")
            st.error(f"❌ Error loading revenue analysis: {e}")
    
    def _show_comprehensive_overview(self):
        """Show comprehensive overview of all analytics."""
        st.subheader("📊 Comprehensive Analytics Overview")
        logger.info("📊 Admin Interface: Loading comprehensive overview")
        
        try:
            # Get all analytics data
            product_winners_df = self.get_mongo_data("product_winners")
            loss_risk_df = self.get_mongo_data("loss_risk_products")
            clients_df = self.get_mongo_data("clients")
            inventory_df = self.get_mongo_data("inventory")
            
            # Create summary dashboard
            st.write("### 📈 Key Performance Indicators")
            
            # Calculate KPIs
            kpi_data = []
            
            # Product performance KPIs
            if not product_winners_df.empty:
                total_profit = product_winners_df['total_profit'].sum()
                avg_profit = product_winners_df['total_profit'].mean()
                kpi_data.append({"Category": "Products", "Metric": "Total Profit", "Value": f"€{total_profit:,.2f}"})
                kpi_data.append({"Category": "Products", "Metric": "Avg Profit", "Value": f"€{avg_profit:,.2f}"})
                kpi_data.append({"Category": "Products", "Metric": "Top Products", "Value": len(product_winners_df)})
            
            # Risk KPIs
            if not loss_risk_df.empty:
                kpi_data.append({"Category": "Risk", "Metric": "At Risk Products", "Value": len(loss_risk_df)})
            else:
                kpi_data.append({"Category": "Risk", "Metric": "At Risk Products", "Value": "0 (Good!)"})
            
            # Client KPIs
            if not clients_df.empty:
                total_revenue = clients_df['total_revenue_generated'].sum()
                avg_revenue = clients_df['total_revenue_generated'].mean()
                kpi_data.append({"Category": "Clients", "Metric": "Total Revenue", "Value": f"€{total_revenue:,.2f}"})
                kpi_data.append({"Category": "Clients", "Metric": "Avg Revenue", "Value": f"€{avg_revenue:,.2f}"})
                kpi_data.append({"Category": "Clients", "Metric": "Total Clients", "Value": len(clients_df)})
            
            # Inventory KPIs
            if not inventory_df.empty:
                total_quantity = inventory_df['quantity'].sum()
                unique_products = inventory_df['product_id'].nunique()
                kpi_data.append({"Category": "Inventory", "Metric": "Total Quantity", "Value": total_quantity})
                kpi_data.append({"Category": "Inventory", "Metric": "Unique Products", "Value": unique_products})
            
            # Display KPIs in a nice table
            kpi_df = pd.DataFrame(kpi_data)
            st.dataframe(kpi_df, use_container_width=True, hide_index=True)
            
            # Create visual dashboard
            st.write("### 📊 Visual Analytics Dashboard")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("#### 🏆 Top Products by Profit")
                if not product_winners_df.empty:
                    st.bar_chart(product_winners_df.head(5).set_index('name')['total_profit'])
                else:
                    st.info("No product data available")
                
                st.write("#### ⚠️ Risk Products by Category")
                if not loss_risk_df.empty and 'category' in loss_risk_df.columns:
                    st.bar_chart(loss_risk_df['category'].value_counts())
                else:
                    st.success("No risk products!")
            
            with col2:
                st.write("#### 💸 Revenue by Client Tier")
                if not clients_df.empty and 'loyalty_tier' in clients_df.columns:
                    tier_revenue = clients_df.groupby('loyalty_tier')['total_revenue_generated'].sum()
                    st.bar_chart(tier_revenue)
                else:
                    st.info("No client revenue data")
                
                st.write("#### 📦 Inventory by Category")
                if not inventory_df.empty and 'category' in inventory_df.columns:
                    category_counts = inventory_df['category'].value_counts()
                    st.bar_chart(category_counts)
                else:
                    st.info("No inventory data")
            
            # Show data health indicators
            st.write("### 🏥 Data Health Indicators")
            
            health_data = []
            
            # Check if we have data in key collections
            collections_status = {
                "Product Winners": not product_winners_df.empty,
                "Loss Risk Products": not loss_risk_df.empty,
                "Clients": not clients_df.empty,
                "Inventory": not inventory_df.empty
            }
            
            for collection, has_data in collections_status.items():
                status = "🟢 Healthy" if has_data else "🔴 Empty"
                health_data.append({"Collection": collection, "Status": status})
            
            health_df = pd.DataFrame(health_data)
            st.dataframe(health_df, use_container_width=True, hide_index=True)
            
            # Show recommendations
            st.write("### 💡 System Recommendations")
            
            recommendations = []
            
            if product_winners_df.empty:
                recommendations.append("⚠️ No product performance data - ensure Spark is processing sales events")
            
            if not loss_risk_df.empty:
                recommendations.append(f"⚠️ {len(loss_risk_df)} products at risk - review inventory management")
            
            if clients_df.empty:
                recommendations.append("⚠️ No client data - ensure client events are being processed")
            
            if inventory_df.empty:
                recommendations.append("⚠️ No inventory data - ensure inventory events are being processed")
            
            if not recommendations:
                recommendations.append("✅ All systems operating normally - no immediate concerns")
            
            for recommendation in recommendations:
                st.info(recommendation)
                
        except Exception as e:
            logger.error(f"❌ Admin Interface: Failed to load comprehensive overview: {e}")
            st.error(f"❌ Error loading comprehensive overview: {e}")
    
    def show_client_promotions_tab(self):
        """Show the Client Promotions tab.
        
        This reads ONLY from MongoDB - completely decoupled from Spark.
        Shows last known results even if no new data arrives.
        """
        st.subheader("💸 Client Loyalty & Promotion Eligibility")
        logger.info("📊 Admin Interface: Loading client data from MongoDB...")
        
        clients_df = self.get_mongo_data("clients")
        
        if not clients_df.empty:
            logger.info(f"📈 Admin Interface: Displaying {len(clients_df)} clients with promotion eligibility")
            
            # Highlight clients eligible for promotion
            def highlight_promo(row):
                return ['background-color: #d4edda'] * len(row) if row.get("eligible_promo") else [''] * len(row)
            
            st.dataframe(clients_df.style.apply(highlight_promo, axis=1), use_container_width=True)
        else:
            logger.info("📉 Admin Interface: No client data available - showing placeholder")
            st.info("📊 Awaiting real-time data... No client information available yet.")
    
    def shutdown(self):
        """Cleanly shutdown the user interface."""
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
        
        if self.mongodb_accountant_manager:
            self.mongodb_accountant_manager.shutdown()
            self.mongodb_accountant_manager = None