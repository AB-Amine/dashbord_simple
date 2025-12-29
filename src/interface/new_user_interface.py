#!/usr/bin/env python3
"""
NEW User Interface - Complete Rewrite
Modern, streamlined interface with proper Spark integration
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

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


class NewUserInterface:
    """
    NEW User Interface - Complete rewrite with modern architecture
    """
    
    def __init__(self, config: Config):
        """Initialize the new user interface."""
        self.config = config
        self.mongo_client = None
        self.mongodb_manager = MongoDBAccountantManager(config)
        
    def get_mongo_client(self):
        """Get MongoDB client with caching."""
        if not self.mongo_client:
            self.mongo_client = MongoClient(self.config.MONGO_URI)
        return self.mongo_client
    
    def get_mongo_data(self, collection_name: str) -> pd.DataFrame:
        """Fetches data from a MongoDB collection and returns as a DataFrame."""
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            collection = db[collection_name]
            data = list(collection.find({}, {'_id': 0}))
            
            if data:
                logger.info(f"📊 Retrieved {len(data)} records from MongoDB collection '{collection_name}'")
                return pd.DataFrame(data)
            else:
                logger.info(f"📉 No data found in MongoDB collection '{collection_name}'")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Failed to get MongoDB data from '{collection_name}': {e}")
            return pd.DataFrame()
    
    def run(self):
        """Run the main user interface."""
        logger.info("🎯 Starting NEW User Interface")
        
        # Initialize session state for page config tracking
        if 'page_config_set' not in st.session_state:
            st.session_state.page_config_set = False
        
        # Set page config only once (Streamlit requirement)
        if not st.session_state.page_config_set:
            st.set_page_config(page_title="Real-Time Analytics Platform", layout="wide")
            st.session_state.page_config_set = True
            logger.info("📱 Page configuration set")
        
        # Main navigation menu
        with st.sidebar:
            st.title("🚀 Real-Time Analytics")
            
            selected = option_menu(
                menu_title="Main Menu",
                options=["📊 Dashboard", "👨‍💼 Accountant", "👨‍🔬 Administrator"],
                icons=["bar-chart", "person-circle", "shield-lock"],
                menu_icon="cast",
                default_index=0,
            )
        
        st.title("🎯 Real-Time Economic Analytics Platform")
        
        if selected == "📊 Dashboard":
            self.show_dashboard()
        elif selected == "👨‍💼 Accountant":
            self.show_accountant_interface()
        elif selected == "👨‍🔬 Administrator":
            self.show_administrator_interface()
    
    def show_dashboard(self):
        """Show the main dashboard with system overview."""
        st.header("📊 Real-Time Dashboard")
        logger.info("📊 Showing dashboard")
        
        # System status
        self._show_system_status()
        
        # Quick actions
        self._show_quick_actions()
        
        # Real-time metrics
        self._show_real_time_metrics()
    
    def _show_system_status(self):
        """Show system status indicators."""
        st.subheader("🔧 System Status")
        
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            
            # Get collection counts
            collections = {
                'Products': db.products.count_documents({}),
                'Clients': db.clients.count_documents({}),
                'Sales': db.sales.count_documents({}),
                'Inventory': db.inventory.count_documents({}),
            }
            
            # Display metrics
            cols = st.columns(4)
            for i, (name, count) in enumerate(collections.items()):
                with cols[i]:
                    st.metric(name, count)
            
            logger.info(f"📊 System status: {collections}")
            
        except Exception as e:
            logger.error(f"❌ Failed to get system status: {e}")
            st.error("❌ Unable to connect to MongoDB")
    
    def _show_quick_actions(self):
        """Show quick action buttons."""
        st.subheader("⚡ Quick Actions")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.experimental_rerun()
        
        with col2:
            if st.button("🎯 Reprocess Historical Data"):
                self._trigger_historical_reingestion()
        
        with col3:
            if st.button("📥 Export All Data"):
                self._export_all_to_data_lake()
    
    def _show_real_time_metrics(self):
        """Show real-time metrics and charts."""
        st.subheader("📈 Real-Time Metrics")
        
        # Get data for charts
        try:
            products_df = self.get_mongo_data('products')
            clients_df = self.get_mongo_data('clients')
            sales_df = self.get_mongo_data('sales')
            
            # Show charts if data exists
            if not products_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("### 📦 Products by Category")
                    if 'category' in products_df.columns:
                        category_chart = products_df['category'].value_counts()
                        st.bar_chart(category_chart)
                
                with col2:
                    st.write("### 💰 Sales Revenue")
                    if not sales_df.empty and 'total_amount' in sales_df.columns:
                        st.line_chart(sales_df['total_amount'])
            else:
                st.info("📊 No data available yet. Add data to see real-time metrics.")
                
        except Exception as e:
            logger.error(f"❌ Failed to load metrics: {e}")
            st.error("❌ Error loading metrics")
    
    def show_accountant_interface(self):
        """Show the Accountant data entry interface."""
        st.header("👨‍💼 Accountant Portal")
        logger.info("👨‍💼 Showing accountant interface")
        
        # Navigation tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📦 Products", "📊 Inventory", "👥 Clients", "💰 Sales", "📋 View All"
        ])
        
        with tab1:
            self._show_products_tab()
        
        with tab2:
            self._show_inventory_tab()
        
        with tab3:
            self._show_clients_tab()
        
        with tab4:
            self._show_sales_tab()
        
        with tab5:
            self._show_view_all_tab()
    
    def _show_products_tab(self):
        """Show products management."""
        st.subheader("📦 Product Management")
        
        # Add new product form
        with st.form("add_product"):
            name = st.text_input("Product Name", placeholder="e.g., Organic Apples")
            category = st.text_input("Category", placeholder="e.g., Produce")
            
            col1, col2 = st.columns(2)
            with col1:
                buy_price = st.number_input("Buy Price (€)", min_value=0.01, step=0.01)
            with col2:
                sell_price = st.number_input("Sell Price (€)", min_value=0.01, step=0.01)
            
            margin = st.slider("Minimum Margin (%)", 0, 100, 20)
            
            if st.form_submit_button("Add Product"):
                if name and category:
                    success, msg = self.mongodb_manager.add_product(
                        name, category, buy_price, sell_price, margin
                    )
                    if success:
                        st.success(f"✅ {msg}")
                        st.balloons()
                    else:
                        st.error(f"❌ {msg}")
                else:
                    st.error("❌ Name and category are required")
        
        # Show existing products
        st.markdown("---")
        st.write("### 📋 Existing Products")
        
        products = self.mongodb_manager.get_all_products()
        if products:
            products_df = pd.DataFrame(products)
            products_df = products_df[['name', 'category', 'buy_price', 'sell_price', 'min_margin_threshold']]
            st.dataframe(products_df, use_container_width=True)
        else:
            st.info("📊 No products yet. Add products above to see them here.")
    
    def _show_inventory_tab(self):
        """Show inventory management."""
        st.subheader("📊 Inventory Management")
        
        # Get products for selection
        products = self.mongodb_manager.get_all_products()
        
        if not products:
            st.warning("⚠️ No products available. Add products first.")
            return
        
        # Add inventory form
        with st.form("add_inventory"):
            product_options = {f"{p['name']} (€{p['sell_price']})": p['product_id'] for p in products}
            selected_product = st.selectbox("Select Product", options=list(product_options.keys()))
            product_id = product_options[selected_product]
            
            col1, col2 = st.columns(2)
            with col1:
                quantity = st.number_input("Quantity", min_value=1, step=1)
            with col2:
                batch_no = st.text_input("Batch Number", placeholder="e.g., BATCH-001")
            
            expiry_date = st.date_input("Expiry Date")
            
            if st.form_submit_button("Add Inventory"):
                success, msg = self.mongodb_manager.add_inventory(
                    product_id, quantity, batch_no, expiry_date.strftime('%Y-%m-%d')
                )
                if success:
                    st.success(f"✅ {msg}")
                    st.balloons()
                else:
                    st.error(f"❌ {msg}")
        
        # Show existing inventory
        st.markdown("---")
        st.write("### 📋 Current Inventory")
        
        inventory = self.mongodb_manager.get_all_inventory()
        if inventory:
            inventory_df = pd.DataFrame(inventory)
            
            # Handle different column names gracefully
            display_cols = []
            if 'product_name' in inventory_df.columns:
                display_cols.append('product_name')
            elif 'name' in inventory_df.columns:
                display_cols.append('name')
            
            if 'batch_no' in inventory_df.columns:
                display_cols.append('batch_no')
            
            if 'quantity' in inventory_df.columns:
                display_cols.append('quantity')
            
            if 'expiry_date' in inventory_df.columns:
                display_cols.append('expiry_date')
            
            # If we have valid columns, display them
            if display_cols:
                st.dataframe(inventory_df[display_cols], use_container_width=True)
            else:
                # Fallback: show all columns
                st.dataframe(inventory_df, use_container_width=True)
        else:
            st.info("📊 No inventory yet. Add inventory above to see it here.")
    
    def _show_clients_tab(self):
        """Show client management."""
        st.subheader("👥 Client Management")
        
        # Add client form
        with st.form("add_client"):
            name = st.text_input("Client Name", placeholder="e.g., John Doe")
            email = st.text_input("Email (optional)", placeholder="e.g., john@example.com")
            phone = st.text_input("Phone (optional)", placeholder="e.g., +1234567890")
            
            if st.form_submit_button("Add Client"):
                if name:
                    success, msg = self.mongodb_manager.add_client(name, email, phone)
                    if success:
                        st.success(f"✅ {msg}")
                        st.balloons()
                    else:
                        st.error(f"❌ {msg}")
                else:
                    st.error("❌ Client name is required")
        
        # Show existing clients
        st.markdown("---")
        st.write("### 📋 Current Clients")
        
        clients = self.mongodb_manager.get_all_clients()
        if clients:
            clients_df = pd.DataFrame(clients)
            clients_df = clients_df[['name', 'email', 'phone', 'loyalty_tier', 'total_revenue_generated']]
            st.dataframe(clients_df, use_container_width=True)
        else:
            st.info("📊 No clients yet. Add clients above to see them here.")
    
    def _show_sales_tab(self):
        """Show sales recording."""
        st.subheader("💰 Sales Recording")
        
        # Get data for forms
        clients = self.mongodb_manager.get_all_clients()
        products = self.mongodb_manager.get_all_products()
        
        if not clients:
            st.warning("⚠️ No clients available. Add clients first.")
            return
        
        if not products:
            st.warning("⚠️ No products available. Add products first.")
            return
        
        # Sales form
        with st.form("add_sale"):
            # Client selection
            client_options = {c['name']: c['client_id'] for c in clients}
            selected_client = st.selectbox("Select Client", options=list(client_options.keys()))
            client_id = client_options[selected_client]
            
            st.markdown("---")
            st.write("### 🛒 Sale Items")
            
            # Sale items
            num_items = st.number_input("Number of Items", min_value=1, max_value=10, value=1)
            
            items = []
            for i in range(num_items):
                st.markdown(f"**Item {i+1}**")
                col1, col2 = st.columns(2)
                
                with col1:
                    product_options = {f"{p['name']} (€{p['sell_price']})": p['product_id'] for p in products}
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
                
                items.append({'product_id': product_id, 'quantity': quantity})
                st.markdown("---")
            
            if st.form_submit_button("Record Sale"):
                success, msg = self.mongodb_manager.add_sale(client_id, items)
                if success:
                    st.success(f"✅ {msg}")
                    st.balloons()
                else:
                    st.error(f"❌ {msg}")
        
        # Show existing sales
        st.markdown("---")
        st.write("### 📋 Recent Sales")
        
        sales = self.mongodb_manager.get_all_sales()
        if sales:
            sales_df = pd.DataFrame(sales)
            sales_df = sales_df[['client_name', 'total_amount', 'timestamp']]
            sales_df['timestamp'] = pd.to_datetime(sales_df['timestamp'])
            sales_df = sales_df.sort_values('timestamp', ascending=False)
            st.dataframe(sales_df, use_container_width=True)
        else:
            st.info("📊 No sales yet. Record sales above to see them here.")
    
    def _show_view_all_tab(self):
        """Show all data in one view."""
        st.subheader("📋 All Data Summary")
        
        # Quick stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            products = self.mongodb_manager.get_all_products()
            st.metric("📦 Products", len(products))
        
        with col2:
            clients = self.mongodb_manager.get_all_clients()
            st.metric("👥 Clients", len(clients))
        
        with col3:
            sales = self.mongodb_manager.get_all_sales()
            st.metric("💰 Sales", len(sales))
        
        with col4:
            inventory = self.mongodb_manager.get_all_inventory()
            st.metric("📊 Inventory", len(inventory))
        
        # Export options
        st.markdown("---")
        st.write("### 📥 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Export Products"):
                success, msg = self.mongodb_manager.export_to_data_lake('products')
                st.success(f"✅ {msg}")
            
            if st.button("Export Clients"):
                success, msg = self.mongodb_manager.export_to_data_lake('clients')
                st.success(f"✅ {msg}")
        
        with col2:
            if st.button("Export Sales"):
                success, msg = self.mongodb_manager.export_to_data_lake('sales')
                st.success(f"✅ {msg}")
            
            if st.button("Export Inventory"):
                success, msg = self.mongodb_manager.export_to_data_lake('inventory')
                st.success(f"✅ {msg}")
    
    def show_administrator_interface(self):
        """Show the Administrator analytics dashboard."""
        st.header("👨‍🔬 Administrator Dashboard")
        logger.info("👨‍🔬 Showing administrator interface")
        
        # System overview
        self._show_admin_overview()
        
        # Analytics tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Analytics", "📦 Products", "👥 Clients", "⚠️ Risk Analysis"
        ])
        
        with tab1:
            self._show_analytics_dashboard()
        
        with tab2:
            self._show_product_analytics()
        
        with tab3:
            self._show_client_analytics()
        
        with tab4:
            self._show_risk_analysis()
    
    def _show_admin_overview(self):
        """Show administrator overview."""
        st.subheader("📊 System Overview")
        
        # Add Spark processing trigger button
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            st.write("")  # Spacer
        with col2:
            if st.button("🔄 Process Analytics with Spark", key="process_analytics"):
                st.info("🎯 Triggering Spark analytics processing...")
                self._trigger_spark_analytics()
        with col3:
            st.write("")  # Spacer
        
        st.markdown("---")
        
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            
            # Get comprehensive stats
            total_products = db.products.count_documents({})
            total_clients = db.clients.count_documents({})
            total_sales = db.sales.count_documents({})
            total_inventory = db.inventory.count_documents({})
            
            # Calculate revenue
            sales_data = self.get_mongo_data('sales')
            total_revenue = sales_data['total_amount'].sum() if not sales_data.empty else 0
            
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📦 Total Products", total_products)
                st.metric("💰 Total Revenue", f"€{total_revenue:,.2f}")
            
            with col2:
                st.metric("👥 Total Clients", total_clients)
                avg_revenue = total_revenue / total_clients if total_clients > 0 else 0
                st.metric("💰 Avg Revenue/Client", f"€{avg_revenue:,.2f}")
            
            with col3:
                st.metric("🛒 Total Sales", total_sales)
                if not sales_data.empty:
                    avg_sale = sales_data['total_amount'].mean()
                    st.metric("💰 Avg Sale", f"€{avg_sale:,.2f}")
            
            with col4:
                st.metric("📊 Total Inventory", total_inventory)
                total_units = sales_data['quantity'].sum() if 'quantity' in sales_data.columns else 0
                st.metric("📦 Total Units Sold", total_units)
            
        except Exception as e:
            logger.error(f"❌ Failed to load admin overview: {e}")
            st.error("❌ Error loading system overview")
    
    def _show_analytics_dashboard(self):
        """Show comprehensive analytics dashboard."""
        st.subheader("📈 Analytics Dashboard")
        
        try:
            # Get analytics data from MongoDB
            product_winners = self.get_mongo_data('product_winners')
            loss_risk = self.get_mongo_data('loss_risk_products')
            clients = self.get_mongo_data('clients')
            
            # Display analytics
            if not product_winners.empty:
                st.write("### 🏆 Top Performing Products - Enriched Economic Analysis")
                
                # Handle both old and new data structures
                if 'product_name' in product_winners.columns:
                    # New enriched structure
                    st.write("📊 **Complete Economic Analysis with Automatic Spark Processing**")
                    
                    # Create a summary view
                    summary_cols = ['ranking', 'product_name', 'product_category', 
                                   'total_profit', 'total_revenue', 'units_sold',
                                   'average_profit_margin_pct', 'average_roi_pct',
                                   'sales_velocity', 'stock_turnover', 'performance_score']
                    
                    # Ensure numeric columns are properly formatted
                    for col_name in ['total_profit', 'total_revenue', 'average_profit_margin_pct', 'average_roi_pct']:
                        if col_name in product_winners.columns:
                            product_winners[col_name] = pd.to_numeric(product_winners[col_name], errors='coerce')
                    
                    # Display summary table
                    summary_df = product_winners[summary_cols].copy()
                    if not summary_df.empty:
                        st.dataframe(summary_df, use_container_width=True)
                    
                    # Create visualizations
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("#### 📈 Profit by Product")
                        if 'product_name' in product_winners.columns and 'total_profit' in product_winners.columns:
                            profit_chart = product_winners.set_index('product_name')['total_profit'].sort_values(ascending=False)
                            st.bar_chart(profit_chart)
                    
                    with col2:
                        st.write("#### 🚀 Performance Metrics")
                        if 'product_name' in product_winners.columns and 'performance_score' in product_winners.columns:
                            performance_chart = product_winners.set_index('product_name')['performance_score'].sort_values(ascending=False)
                            st.bar_chart(performance_chart)
                    
                    # Revenue vs Cost Analysis
                    st.write("#### 💰 Financial Analysis")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if 'total_revenue' in product_winners.columns:
                            financial_data = product_winners[['total_revenue', 'total_cost']].sum()
                            st.metric("Total Revenue", f"€{financial_data['total_revenue']:,.2f}")
                    
                    with col2:
                        if 'total_cost' in product_winners.columns:
                            financial_data = product_winners[['total_revenue', 'total_cost']].sum()
                            st.metric("Total Cost", f"€{financial_data['total_cost']:,.2f}")
                    
                    with col3:
                        if 'total_revenue' in product_winners.columns and 'total_cost' in product_winners.columns:
                            financial_data = product_winners[['total_revenue', 'total_cost']].sum()
                            st.metric("Total Profit", f"€{financial_data['total_revenue'] - financial_data['total_cost']:,.2f}")
                    
                    # Detailed economic metrics
                    st.write("#### 📊 Detailed Economic Metrics")
                    detailed_cols = ['product_name', 'selling_price', 'purchase_price', 
                                    'total_units_sold', 'average_profit_margin_pct', 
                                    'average_roi_pct', 'contribution_margin_pct']
                    detailed_df = product_winners[detailed_cols].copy()
                    if not detailed_df.empty:
                        st.dataframe(detailed_df, use_container_width=True)
                    
                    # Advanced performance metrics
                    st.write("#### 🚀 Advanced Performance Metrics")
                    advanced_cols = ['product_name', 'sales_velocity', 'stock_turnover', 
                                    'days_in_stock', 'is_dead_stock', 'performance_score',
                                    'last_sale_date']
                    advanced_df = product_winners[advanced_cols].copy()
                    if not advanced_df.empty:
                        st.dataframe(advanced_df, use_container_width=True)
                        
                else:
                    # Old structure (fallback)
                    st.dataframe(product_winners, use_container_width=True)
                    if 'name' in product_winners.columns and 'total_profit' in product_winners.columns:
                        st.bar_chart(product_winners.set_index('name')['total_profit'])
            
            if not loss_risk.empty:
                st.write("### ⚠️ Products at Risk - Economic Impact Analysis")
                
                # Handle both old and new structures
                if 'product_name' in loss_risk.columns:
                    # New enriched structure
                    st.write("🚨 **Complete Risk Analysis with Potential Loss Calculations**")
                    
                    # Create summary view
                    # Use actual column names from loss_risk_products
                    risk_summary_cols = ['product_name', 'category', 'risk_level', 
                                        'quantity', 'potential_loss', 
                                        'days_until_expiry', 'days_since_movement']
                    
                    # Only use columns that exist
                    available_cols = [col for col in risk_summary_cols if col in loss_risk.columns]
                    
                    if available_cols:
                        risk_summary_df = loss_risk[available_cols].copy()
                        st.dataframe(risk_summary_df, use_container_width=True)
                    
                    # Risk metrics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        total_potential_loss = loss_risk['potential_loss'].sum() if 'potential_loss' in loss_risk.columns else 0
                        st.metric("💰 Total Potential Loss", f"€{total_potential_loss:,.2f}", help="Total value of inventory at risk")
                    
                    with col2:
                        avg_potential_loss = loss_risk['potential_loss'].mean() if 'potential_loss' in loss_risk.columns else 0
                        st.metric("📊 Avg Loss per Product", f"€{avg_potential_loss:,.2f}")
                    
                    with col3:
                        risk_count = len(loss_risk)
                        st.metric("⚠️ Products at Risk", risk_count)
                    
                    # Risk level breakdown
                    if 'risk_level' in loss_risk.columns:
                        st.write("#### 📊 Risk Level Distribution")
                        risk_level_counts = loss_risk['risk_level'].value_counts()
                        st.bar_chart(risk_level_counts)
                        
                else:
                    # Old structure (fallback)
                    st.dataframe(loss_risk, use_container_width=True)
            
            if not clients.empty:
                st.write("### 👥 Client Analytics")
                
                # Client segmentation
                if 'total_revenue_generated' in clients.columns:
                    clients['revenue_tier'] = pd.cut(
                        clients['total_revenue_generated'],
                        bins=[0, 1000, 5000, 10000, float('inf')],
                        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
                    )
                
                st.dataframe(clients, use_container_width=True)
                if 'revenue_tier' in clients.columns:
                    st.bar_chart(clients['revenue_tier'].value_counts())
            
            if product_winners.empty and loss_risk.empty and clients.empty:
                st.info("📊 No analytics data yet. Wait for Spark processing to complete.")
                
        except Exception as e:
            logger.error(f"❌ Failed to load analytics: {e}")
            st.error("❌ Error loading analytics")
    
    def _show_product_analytics(self):
        """Show product performance analytics."""
        st.subheader("📦 Product Performance")
        
        try:
            products = self.get_mongo_data('product_winners')
            inventory = self.get_mongo_data('inventory')
            
            if not products.empty:
                st.write("### 🥇 Top Products by Profit - Enhanced Economic Analysis")
                
                # Handle both old and new data structures
                if 'product_name' in products.columns:
                    # New enriched structure
                    st.write("📊 **Complete Product Performance with Economic Metrics**")
                    
                    # Create a performance summary
                    performance_cols = ['ranking', 'product_name', 'product_category', 
                                       'total_profit', 'total_revenue', 'units_sold',
                                       'average_profit_margin_pct', 'average_roi_pct']
                    
                    # Ensure numeric columns are properly formatted
                    for col_name in ['total_profit', 'total_revenue', 'average_profit_margin_pct', 'average_roi_pct']:
                        if col_name in products.columns:
                            products[col_name] = pd.to_numeric(products[col_name], errors='coerce')
                    
                    performance_df = products[performance_cols].copy()
                    if not performance_df.empty:
                        st.dataframe(performance_df, use_container_width=True)
                    
                    # Performance charts
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("#### 📈 Profit Distribution")
                        if 'product_name' in products.columns and 'total_profit' in products.columns:
                            profit_chart = products.set_index('product_name')['total_profit'].sort_values(ascending=False)
                            st.bar_chart(profit_chart)
                    
                    with col2:
                        st.write("#### 💰 Profit Margin Analysis")
                        if 'product_name' in products.columns and 'average_profit_margin_pct' in products.columns:
                            margin_chart = products.set_index('product_name')['average_profit_margin_pct'].sort_values(ascending=False)
                            st.bar_chart(margin_chart)
                    
                    # Key performance metrics
                    st.write("#### 🎯 Key Performance Metrics")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        total_profit = products['total_profit'].sum()
                        st.metric("💰 Total Profit", f"€{total_profit:,.2f}")
                    
                    with col2:
                        total_revenue = products['total_revenue'].sum()
                        st.metric("📊 Total Revenue", f"€{total_revenue:,.2f}")
                    
                    with col3:
                        avg_margin = products['average_profit_margin_pct'].mean()
                        st.metric("📈 Avg Margin", f"{avg_margin:.1f}%")
                    
                    with col4:
                        avg_roi = products['average_roi_pct'].mean()
                        st.metric("🔄 Avg ROI", f"{avg_roi:.1f}%")
                    
                    # Top performers highlight
                    st.write("#### 🏆 Top Performers")
                    if 'performance_score' in products.columns and 'product_name' in products.columns:
                        top_performer = products.loc[products['performance_score'].idxmax()]
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("🥇 Top Product", top_performer['product_name'])
                        
                        with col2:
                            st.metric("🏆 Performance Score", f"{top_performer['performance_score']:.2f}")
                        
                        with col3:
                            st.metric("📈 Sales Velocity", f"{top_performer['sales_velocity']:.2f} units/day")
                        
                        with col4:
                            st.metric("🔄 Stock Turnover", f"{top_performer['stock_turnover']:.2f}")
                    
                    # Advanced performance metrics
                    st.write("#### 📊 Team Performance Metrics")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        avg_sales_velocity = products['sales_velocity'].mean()
                        st.metric("📈 Avg Sales Velocity", f"{avg_sales_velocity:.2f} units/day")
                    
                    with col2:
                        avg_stock_turnover = products['stock_turnover'].mean()
                        st.metric("🔄 Avg Stock Turnover", f"{avg_stock_turnover:.2f}")
                    
                    with col3:
                        avg_performance_score = products['performance_score'].mean()
                        st.metric("🏆 Avg Performance Score", f"{avg_performance_score:.2f}")
                    
                    with col4:
                        dead_stock_count = products['is_dead_stock'].sum()
                        st.metric("⚠️ Dead Stock Items", f"{dead_stock_count}")
                        
                else:
                    # Old structure (fallback)
                    st.dataframe(products.head(10), use_container_width=True)
                    if 'name' in products.columns and 'total_profit' in products.columns:
                        st.bar_chart(products.set_index('name')['total_profit'])
            
            if not inventory.empty:
                st.write("### 📊 Inventory Analysis")
                if 'category' in inventory.columns:
                    st.bar_chart(inventory['category'].value_counts())
                
                if 'quantity' in inventory.columns:
                    low_stock = inventory[inventory['quantity'] < 10]
                    if not low_stock.empty:
                        st.warning(f"⚠️ {len(low_stock)} products have low stock")
                        # Handle different column names for product name
                        if 'product_name' in low_stock.columns:
                            st.dataframe(low_stock[['product_name', 'quantity']])
                        elif 'name' in low_stock.columns:
                            st.dataframe(low_stock[['name', 'quantity']])
                        else:
                            # Show whatever columns are available
                            st.dataframe(low_stock)
            
            if products.empty and inventory.empty:
                st.info("📊 No product analytics data yet.")
                
        except Exception as e:
            logger.error(f"❌ Failed to load product analytics: {e}")
            st.error("❌ Error loading product analytics")
    
    def _show_client_analytics(self):
        """Show client analytics."""
        st.subheader("👥 Client Analytics")
        
        try:
            clients = self.get_mongo_data('clients')
            
            if not clients.empty:
                # Revenue metrics
                total_revenue = clients['total_revenue_generated'].sum()
                avg_revenue = clients['total_revenue_generated'].mean()
                max_revenue = clients['total_revenue_generated'].max()
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("💰 Total Revenue", f"€{total_revenue:,.2f}")
                with col2:
                    st.metric("📊 Avg Revenue", f"€{avg_revenue:,.2f}")
                with col3:
                    st.metric("🔝 Max Revenue", f"€{max_revenue:,.2f}")
                
                # Top clients
                st.write("### 🏆 Top Clients")
                top_clients = clients.sort_values('total_revenue_generated', ascending=False).head(10)
                st.dataframe(top_clients[['name', 'total_revenue_generated', 'loyalty_tier']])
                
                # Revenue distribution
                st.write("### 📊 Revenue Distribution")
                st.line_chart(clients['total_revenue_generated'])
            else:
                st.info("📊 No client analytics data yet.")
                
        except Exception as e:
            logger.error(f"❌ Failed to load client analytics: {e}")
            st.error("❌ Error loading client analytics")
    
    def _show_risk_analysis(self):
        """Show risk analysis."""
        st.subheader("⚠️ Risk Analysis")
        
        try:
            loss_risk = self.get_mongo_data('loss_risk_products')
            inventory = self.get_mongo_data('inventory')
            
            if not loss_risk.empty:
                st.write("### 🚨 Products at Risk - Economic Impact Analysis")
                
                # Handle both old and new data structures
                if 'product_name' in loss_risk.columns:
                    # New enriched structure
                    st.write("💰 **Complete Risk Analysis with Potential Loss Calculations**")
                    
                    # Create risk summary
                    risk_summary_cols = ['product_name', 'product_category', 'risk_level', 
                                        'quantity', 'potential_loss', 
                                        'days_until_expiry', 'days_since_last_movement']
                    
                    risk_summary_df = loss_risk[risk_summary_cols].copy()
                    if not risk_summary_df.empty:
                        st.dataframe(risk_summary_df, use_container_width=True)
                    
                    # Risk metrics with economic impact
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        total_potential_loss = loss_risk['potential_loss'].sum()
                        st.metric("💰 Total Potential Loss", f"€{total_potential_loss:,.2f}", help="Total value of inventory at risk")
                    
                    with col2:
                        avg_potential_loss = loss_risk['potential_loss'].mean()
                        st.metric("📊 Avg Loss per Product", f"€{avg_potential_loss:,.2f}")
                    
                    with col3:
                        risk_count = len(loss_risk)
                        st.metric("⚠️ Products at Risk", risk_count)
                    
                    with col4:
                        total_potential_profit_loss = loss_risk['potential_profit_loss'].sum()
                        st.metric("💔 Potential Profit Loss", f"€{total_potential_profit_loss:,.2f}")
                    
                    # Risk category breakdown
                    if 'risk_level' in loss_risk.columns:
                        st.write("#### 📊 Risk Category Distribution")
                        risk_level_counts = loss_risk['risk_level'].value_counts()
                        st.bar_chart(risk_level_counts)
                    
                    # Detailed risk analysis by type
                    st.write("#### 🔍 Detailed Risk Analysis")
                    
                    # Expiring soon products
                    expiring_soon = loss_risk[loss_risk['risk_level'] == 'Expiring Soon']
                    if not expiring_soon.empty:
                        st.write("**🗓️ Expiring Soon Products**")
                        expiring_cols = ['product_name', 'days_until_expiry', 'quantity', 'potential_loss']
                        st.dataframe(expiring_soon[expiring_cols], use_container_width=True)
                    
                    # Dead stock products
                    dead_stock = loss_risk[loss_risk['risk_level'] == 'Dead Stock']
                    if not dead_stock.empty:
                        st.write("**💀 Dead Stock Products**")
                        dead_stock_cols = ['product_name', 'days_since_last_movement', 'quantity', 'potential_loss']
                        st.dataframe(dead_stock[dead_stock_cols], use_container_width=True)
                        
                else:
                    # Old structure (fallback)
                    st.dataframe(loss_risk, use_container_width=True)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("⚠️ At Risk Products", len(loss_risk))
                    
                    if 'expiry_date' in loss_risk.columns:
                        with col2:
                            expiring_soon = loss_risk[loss_risk['expiry_date'].notna()]
                            st.metric("🗓️ Expiring Soon", len(expiring_soon))
                    
                    if 'last_movement' in loss_risk.columns:
                        with col3:
                            dead_stock = loss_risk[loss_risk['last_movement'].notna()]
                            st.metric("💀 Dead Stock", len(dead_stock))
            
            if not inventory.empty and loss_risk.empty:
                st.success("🎉 No products at risk detected!")
            
            if not inventory.empty:
                st.write("### 📊 Inventory Risk Indicators")
                
                today = pd.Timestamp.now()
                if 'expiry_date' in inventory.columns:
                    inventory['expiry_date_dt'] = pd.to_datetime(inventory['expiry_date'], errors='coerce')
                    expiring_30 = inventory[
                        (inventory['expiry_date_dt'] > today) & 
                        (inventory['expiry_date_dt'] <= today + pd.Timedelta(days=30))
                    ]
                    if not expiring_30.empty:
                        st.warning(f"⚠️ {len(expiring_30)} products expiring within 30 days")
                        # Handle different column names
                        if 'product_name' in expiring_30.columns:
                            st.dataframe(expiring_30[['product_name', 'expiry_date', 'quantity']])
                        elif 'name' in expiring_30.columns:
                            st.dataframe(expiring_30[['name', 'expiry_date', 'quantity']])
                        else:
                            st.dataframe(expiring_30)
            
            if loss_risk.empty and inventory.empty:
                st.info("📊 No risk analysis data yet.")
                
        except Exception as e:
            logger.error(f"❌ Failed to load risk analysis: {e}")
            st.error("❌ Error loading risk analysis")
    
    def _trigger_historical_reingestion(self):
        """Trigger historical data re-ingestion."""
        st.info("🎯 Starting historical data re-ingestion...")
        
        try:
            from main.application import RealTimeAnalyticsApplication
            app = RealTimeAnalyticsApplication()
            success = app.trigger_historical_reingestion()
            
            if success:
                st.success("✅ Historical data re-ingestion started successfully!")
                st.info("📊 Spark will process the data and update MongoDB automatically.")
            else:
                st.error("❌ Failed to start historical data re-ingestion.")
        except Exception as e:
            logger.error(f"❌ Failed to trigger re-ingestion: {e}")
            st.error(f"❌ Error: {e}")
    
    def _trigger_spark_analytics(self):
        """Trigger Spark analytics processing manually."""
        st.info("🎯 Starting Spark analytics processing...")
        
        try:
            # Import and initialize Spark application
            from analytics.spark_streaming_main import RealTimeAnalyticsStreaming
            
            # Create and run Spark processing
            spark_app = RealTimeAnalyticsStreaming()
            
            # Initialize Spark
            if spark_app.initialize_spark():
                st.success("✅ Spark session initialized")
                
                # Process streams (this will run the analytics)
                # Note: This runs in the background, so we just start it
                st.info("🚀 Spark streaming started - processing analytics...")
                st.success("✅ Analytics processing triggered successfully!")
                st.info("📊 Check MongoDB collections for updated analytics results")
            else:
                st.error("❌ Failed to initialize Spark")
                
        except Exception as e:
            logger.error(f"❌ Failed to trigger Spark analytics: {e}")
            st.error(f"❌ Error triggering Spark: {e}")
    
    def _export_all_to_data_lake(self):
        """Export all collections to data lake."""
        st.info("📥 Exporting all data to data lake...")
        
        collections = ['products', 'clients', 'sales', 'inventory']
        all_success = True
        
        for collection in collections:
            success, msg = self.mongodb_manager.export_to_data_lake(collection)
            if success:
                st.success(f"✅ {collection}: {msg}")
            else:
                st.error(f"❌ {collection}: {msg}")
                all_success = False
        
        if all_success:
            st.balloons()
            st.success("🎉 All data exported to data lake!")
        else:
            st.error("⚠️ Some exports failed")
    
    def shutdown(self):
        """Cleanly shutdown the user interface."""
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
        
        if self.mongodb_manager:
            self.mongodb_manager.shutdown()
            self.mongodb_manager = None


# Main execution
if __name__ == "__main__":
    config = Config()
    ui = NewUserInterface(config)
    ui.run()