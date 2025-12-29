#!/usr/bin/env python3
"""
Enhanced Admin Interface with Integrated Spark Analytics
This new admin interface includes automatic Spark analysis every 10 seconds
and real-time data transfer from MongoDB to Kafka.
"""

import streamlit as st
import pandas as pd
import threading
import time
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pymongo import MongoClient

# Add src to path for imports
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager
from kafka.kafka_real_time_producer import RealTimeKafkaProducer
from analytics.spark_analytics import SparkAnalytics  # For batch processing

# Configure logging for Enhanced Admin Interface
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EnhancedAdminInterface")

class EnhancedAdminInterface:
    """
    Enhanced Admin Interface with integrated Spark analytics and auto-analysis.
    """
    
    def __init__(self, config: Config):
        """Initialize Enhanced Admin Interface with configuration."""
        self.config = config
        self.mongo_client = None
        self.mongodb_manager = MongoDBAccountantManager(config)
        self.kafka_producer = RealTimeKafkaProducer(config)
        self.spark_analytics = SparkAnalytics(config)
        self.auto_analysis_active = False
        self.auto_analysis_thread = None
        self.kafka_transfer_active = False
        self.kafka_transfer_thread = None
        
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
            data = list(collection.find({}, {'_id': 0}))  # Exclude the default _id field
            
            if data:
                logger.info(f"📊 Enhanced Admin: Retrieved {len(data)} records from '{collection_name}'")
                return pd.DataFrame(data)
            else:
                logger.info(f"📉 Enhanced Admin: No data in collection '{collection_name}'")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Failed to get data from '{collection_name}': {e}")
            return pd.DataFrame()
    
    def start_auto_analysis(self):
        """Start automatic Spark analysis every 10 seconds."""
        if not self.auto_analysis_active:
            self.auto_analysis_active = True
            logger.info("🚀 Enhanced Admin: Starting auto-analysis thread...")
            
            def analysis_loop():
                while self.auto_analysis_active:
                    start_time = time.time()
                    logger.info("⏰ Enhanced Admin: Running scheduled Spark analysis...")
                    
                    try:
                        # Run Spark analytics
                        self.spark_analytics.run_analytics()
                        logger.info("✅ Enhanced Admin: Spark analysis completed successfully")
                        
                    except Exception as e:
                        logger.error(f"❌ Enhanced Admin: Auto-analysis failed: {e}")
                    
                    # Calculate sleep time to maintain 10-second interval
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 10 - elapsed)
                    time.sleep(sleep_time)
            
            self.auto_analysis_thread = threading.Thread(target=analysis_loop, daemon=True)
            self.auto_analysis_thread.start()
            
            return True
        else:
            logger.warning("⚠️  Enhanced Admin: Auto-analysis already running")
            return False
    
    def stop_auto_analysis(self):
        """Stop automatic Spark analysis."""
        if self.auto_analysis_active:
            self.auto_analysis_active = False
            if self.auto_analysis_thread:
                self.auto_analysis_thread.join(timeout=5)
            logger.info("🛑 Enhanced Admin: Auto-analysis stopped")
            return True
        else:
            logger.warning("⚠️  Enhanced Admin: Auto-analysis not running")
            return False
    
    def start_kafka_transfer(self):
        """Start automatic MongoDB to Kafka data transfer."""
        if not self.kafka_transfer_active:
            self.kafka_transfer_active = True
            logger.info("🚀 Enhanced Admin: Starting Kafka transfer thread...")
            
            def transfer_loop():
                while self.kafka_transfer_active:
                    start_time = time.time()
                    logger.info("📥 Enhanced Admin: Running MongoDB to Kafka transfer...")
                    
                    try:
                        # Run Kafka transfer
                        self.kafka_producer.import_data_to_kafka()
                        logger.info("✅ Enhanced Admin: Kafka transfer completed successfully")
                        
                    except Exception as e:
                        logger.error(f"❌ Enhanced Admin: Kafka transfer failed: {e}")
                    
                    # Calculate sleep time to maintain 10-second interval
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 10 - elapsed)
                    time.sleep(sleep_time)
            
            self.kafka_transfer_thread = threading.Thread(target=transfer_loop, daemon=True)
            self.kafka_transfer_thread.start()
            
            return True
        else:
            logger.warning("⚠️  Enhanced Admin: Kafka transfer already running")
            return False
    
    def stop_kafka_transfer(self):
        """Stop automatic Kafka data transfer."""
        if self.kafka_transfer_active:
            self.kafka_transfer_active = False
            if self.kafka_transfer_thread:
                self.kafka_transfer_thread.join(timeout=5)
            logger.info("🛑 Enhanced Admin: Kafka transfer stopped")
            return True
        else:
            logger.warning("⚠️  Enhanced Admin: Kafka transfer not running")
            return False
    
    def run(self):
        """Run the enhanced admin interface."""
        logger.info("🎯 Enhanced Admin Interface: Starting...")
        
        # Initialize session state
        if 'page_config_set' not in st.session_state:
            st.session_state.page_config_set = False
        
        if not st.session_state.page_config_set:
            st.set_page_config(page_title="Enhanced Analytics Admin", layout="wide")
            st.session_state.page_config_set = True
            logger.info("📱 Enhanced Admin: Page configuration set")
        
        st.title("🚀 Enhanced Real-Time Analytics Admin")
        st.markdown("""
        **Next-Generation Analytics Dashboard**
        This enhanced interface provides automatic Spark analytics, real-time data transfer,
        and comprehensive system monitoring with auto-refresh capabilities.
        """)
        
        # Create main tabs
        main_tab1, main_tab2, main_tab3, main_tab4 = st.tabs([
            "📊 Real-Time Dashboard",
            "⚙️ System Control", 
            "🔄 Data Pipeline",
            "📈 Advanced Analytics"
        ])
        
        with main_tab1:
            self._show_real_time_dashboard()
            
        with main_tab2:
            self._show_system_control()
            
        with main_tab3:
            self._show_data_pipeline()
            
        with main_tab4:
            self._show_advanced_analytics()
    
    def _show_real_time_dashboard(self):
        """Show the real-time analytics dashboard."""
        st.header("📊 Real-Time Analytics Dashboard")
        logger.info("📊 Enhanced Admin: Loading real-time dashboard")
        
        # System Status with Auto-Refresh
        st.subheader("🔧 System Status & Auto-Refresh")
        
        # Auto-refresh controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button('🔄 Refresh Now', key='refresh_now'):
                logger.info("🔄 Enhanced Admin: Manual refresh requested")
                st.cache_data.clear()
                st.experimental_rerun()
        
        with col2:
            auto_refresh = st.toggle("⏰ Auto-Refresh (10s)", value=False, key='auto_refresh')
            if auto_refresh:
                time.sleep(10)
                st.experimental_rerun()
        
        with col3:
            st.metric("🕒 Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        # System Metrics
        st.subheader("📈 System Metrics")
        
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            
            # Count documents in key collections
            metrics = {
                "🏆 Product Winners": db.product_winners.count_documents({}),
                "⚠️ Loss Risk Products": db.loss_risk_products.count_documents({}),
                "👥 Active Clients": db.clients.count_documents({}),
                "📦 Inventory Items": db.inventory.count_documents({}),
                "💰 Total Sales": db.sales.count_documents({}),
                "📊 Products": db.products.count_documents({})
            }
            
            # Display metrics in a grid
            col1, col2, col3 = st.columns(3)
            
            for i, (label, value) in enumerate(metrics.items()):
                with [col1, col2, col3][i % 3]:
                    st.metric(label, value)
            
            logger.info(f"📊 Enhanced Admin: System metrics - {metrics}")
            
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Failed to get system metrics: {e}")
            st.error("❌ Unable to connect to MongoDB")
        
        # Real-time Data Flow Visualization
        st.subheader("🌊 Real-Time Data Flow")
        
        st.markdown("""
        ```
        MongoDB → Kafka → Spark Streaming → MongoDB (Analytics)
        """)
        
        # Status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.info("🗄️ MongoDB")
            st.write("Source & Destination")
        
        with col2:
            if self.kafka_transfer_active:
                st.success("📥 Kafka")
            else:
                st.warning("📥 Kafka")
            st.write("Data Transfer")
        
        with col3:
            if self.auto_analysis_active:
                st.success("⚡ Spark")
            else:
                st.warning("⚡ Spark")
            st.write("Streaming Analytics")
        
        with col4:
            st.info("📊 Analytics DB")
            st.write("Results Storage")
    
    def _show_system_control(self):
        """Show system control panel."""
        st.header("⚙️ System Control Center")
        logger.info("🎛️ Enhanced Admin: Loading system control panel")
        
        # Auto-Analysis Controls
        st.subheader("🤖 Auto-Analysis Controls")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Automatic Spark Analysis**
            - Runs every 10 seconds
            - Processes data from Kafka topics
            - Updates analytics collections in MongoDB
            - Includes product performance, loss risk analysis
            """)
            
            if st.button("🚀 Start Auto-Analysis", disabled=self.auto_analysis_active):
                if self.start_auto_analysis():
                    st.success("✅ Auto-analysis started successfully!")
                    st.info("📊 Spark will run analytics every 10 seconds")
                else:
                    st.warning("⚠️  Auto-analysis already running")
        
        with col2:
            st.markdown("""
            **Current Status**
            - Auto-Analysis: **{}**
            - Next Run: **{}**
            - Last Run: **{}**
            """.format(
                "🟢 ACTIVE" if self.auto_analysis_active else "🔴 INACTIVE",
                "In 10 seconds" if self.auto_analysis_active else "N/A",
                datetime.now().strftime("%H:%M:%S") if self.auto_analysis_active else "Never"
            ))
            
            if st.button("🛑 Stop Auto-Analysis", disabled=not self.auto_analysis_active):
                if self.stop_auto_analysis():
                    st.success("✅ Auto-analysis stopped successfully!")
                else:
                    st.warning("⚠️  Auto-analysis not running")
        
        st.markdown("---")
        
        # Kafka Transfer Controls
        st.subheader("📥 Kafka Data Transfer Controls")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Automatic MongoDB → Kafka Transfer**
            - Runs every 10 seconds
            - Transfers products, clients, sales, inventory
            - Feeds data to Spark streaming pipeline
            - Ensures real-time data availability
            """)
            
            if st.button("🚀 Start Kafka Transfer", disabled=self.kafka_transfer_active):
                if self.start_kafka_transfer():
                    st.success("✅ Kafka transfer started successfully!")
                    st.info("📥 Data will be transferred to Kafka every 10 seconds")
                else:
                    st.warning("⚠️  Kafka transfer already running")
        
        with col2:
            st.markdown("""
            **Current Status**
            - Kafka Transfer: **{}**
            - Next Transfer: **{}**
            - Last Transfer: **{}**
            """.format(
                "🟢 ACTIVE" if self.kafka_transfer_active else "🔴 INACTIVE",
                "In 10 seconds" if self.kafka_transfer_active else "N/A",
                datetime.now().strftime("%H:%M:%S") if self.kafka_transfer_active else "Never"
            ))
            
            if st.button("🛑 Stop Kafka Transfer", disabled=not self.kafka_transfer_active):
                if self.stop_kafka_transfer():
                    st.success("✅ Kafka transfer stopped successfully!")
                else:
                    st.warning("⚠️  Kafka transfer not running")
        
        # Manual Controls
        st.subheader("🎛️ Manual Controls")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Run Analysis Now"):
                try:
                    self.spark_analytics.run_analytics()
                    st.success("✅ Spark analysis completed!")
                except Exception as e:
                    st.error(f"❌ Analysis failed: {e}")
        
        with col2:
            if st.button("📥 Transfer to Kafka Now"):
                try:
                    self.kafka_producer.import_data_to_kafka()
                    st.success("✅ Data transferred to Kafka!")
                except Exception as e:
                    st.error(f"❌ Transfer failed: {e}")
        
        with col3:
            if st.button("🎯 Full Pipeline Run"):
                try:
                    # Transfer to Kafka
                    self.kafka_producer.import_data_to_kafka()
                    # Run Spark analytics
                    self.spark_analytics.run_analytics()
                    st.success("✅ Full pipeline completed!")
                    st.info("📊 Data transferred and analyzed successfully")
                except Exception as e:
                    st.error(f"❌ Pipeline failed: {e}")
    
    def _show_data_pipeline(self):
        """Show data pipeline monitoring."""
        st.header("🔄 Data Pipeline Monitoring")
        logger.info("🔄 Enhanced Admin: Loading data pipeline monitoring")
        
        # Pipeline Visualization
        st.subheader("🗺️ Pipeline Architecture")
        
        st.markdown("""
        ```
        [MongoDB Accountant Data]
                    ↓
        [Kafka Real-Time Producer] → [Kafka Topics]
                    ↓
        [Spark Streaming Analytics] → [MongoDB Analytics Collections]
                    ↓
        [Admin Dashboard] ← [Real-Time Updates]
        ```
        """)
        
        # Pipeline Status
        st.subheader("📊 Pipeline Status")
        
        try:
            client = self.get_mongo_client()
            db = client[self.config.DB_NAME]
            
            # Check data in key collections
            source_data = {
                "Products": db.products.count_documents({}),
                "Clients": db.clients.count_documents({}),
                "Sales": db.sales.count_documents({}),
                "Inventory": db.inventory.count_documents({})
            }
            
            analytics_data = {
                "Product Winners": db.product_winners.count_documents({}),
                "Loss Risk Products": db.loss_risk_products.count_documents({})
            }
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📥 Source Data (MongoDB)**")
                for collection, count in source_data.items():
                    st.write(f"- {collection}: {count} records")
            
            with col2:
                st.markdown("**📊 Analytics Results (MongoDB)**")
                for collection, count in analytics_data.items():
                    st.write(f"- {collection}: {count} records")
            
            # Pipeline Health
            total_source = sum(source_data.values())
            total_analytics = sum(analytics_data.values())
            
            if total_source > 0 and total_analytics > 0:
                st.success("✅ Pipeline Healthy: Data flowing correctly")
            elif total_source > 0:
                st.warning("⚠️  Pipeline Active: Waiting for analytics results")
            else:
                st.info("ℹ️ Pipeline Ready: Add data to start processing")
                
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Pipeline monitoring failed: {e}")
            st.error("❌ Unable to monitor pipeline status")
        
        # Kafka Topics Monitoring
        st.subheader("📦 Kafka Topics Monitoring")
        
        st.markdown("""
        **Active Kafka Topics:**
        - `topic-inventory-updates`: Product and inventory data
        - `topic-clients`: Client information and revenue
        - `topic-raw-sales`: Real-time sales transactions
        """)
        
        st.info("📊 Kafka topics are automatically created and managed by the system")
    
    def _show_advanced_analytics(self):
        """Show advanced analytics and insights."""
        st.header("📈 Advanced Analytics & Insights")
        logger.info("📊 Enhanced Admin: Loading advanced analytics")
        
        # Analytics Tabs
        analytics_tab1, analytics_tab2, analytics_tab3 = st.tabs([
            "🏆 Product Performance",
            "⚠️ Risk Analysis", 
            "👥 Client Insights"
        ])
        
        with analytics_tab1:
            self._show_product_performance()
            
        with analytics_tab2:
            self._show_risk_analysis()
            
        with analytics_tab3:
            self._show_client_insights()
    
    def _show_product_performance(self):
        """Show product performance analytics."""
        st.subheader("🏆 Product Performance Analytics")
        
        try:
            # Try to use the product performance analyzer for enriched data
            from data.product_performance_analyzer import ProductPerformanceAnalyzer
            
            analyzer = ProductPerformanceAnalyzer()
            performance_data = analyzer.get_product_performance_data()
            
            if performance_data:
                # Convert to DataFrame for display
                performance_df = pd.DataFrame(performance_data)
                
                # Select columns to display (use available ones)
                display_columns = []
                for col in ['product_id', 'product_name', 'category', 'total_sales', 'total_revenue', 'profit_margin']:
                    if col in performance_df.columns:
                        display_columns.append(col)
                
                if display_columns:
                    st.dataframe(
                        performance_df[display_columns],
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Show top performers
                    st.markdown("**🥇 Top Performing Products**")
                    top_products = analyzer.get_top_performing_products()
                    
                    for product in top_products:
                        product_name = product.get('product_name', product.get('product_id', 'Unknown Product'))
                        revenue = product.get('total_revenue', 0)
                        st.success(f"🏆 {product_name} - €{revenue:.2f} revenue")
                else:
                    st.info("📊 Product performance data available but no displayable columns found")
            else:
                st.info("📊 No product performance data available. This will be populated when sales are recorded.")
                
            analyzer.shutdown()
                
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Failed to load product performance: {e}")
            st.error("❌ Unable to load product performance data")
    
    def _show_risk_analysis(self):
        """Show risk analysis."""
        st.subheader("⚠️ Loss Risk Analysis")
        
        try:
            # Get loss risk products data
            loss_risk = self.get_mongo_data('loss_risk_products')
            
            if not loss_risk.empty:
                # Check which columns are available and handle gracefully
                available_columns = []
                for col in ['product_id', 'product_name', 'category', 'risk_level', 'risk_reason', 'potential_loss', 'days_until_expiry', 'days_since_movement']:
                    if col in loss_risk.columns:
                        available_columns.append(col)
                
                if available_columns:
                    st.dataframe(
                        loss_risk[available_columns],
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Show high risk products
                    st.markdown("**⚠️ High Risk Products**")
                    if 'risk_level' in loss_risk.columns:
                        high_risk = loss_risk[loss_risk['risk_level'] == 'High']
                        
                        for _, product in high_risk.iterrows():
                            product_name = product.get('product_name', product.get('product_id', 'Unknown Product'))
                            risk_level = product.get('risk_level', 'Unknown')
                            risk_reason = product.get('risk_reason', 'No reason specified')
                            potential_loss = product.get('potential_loss', 0)
                            
                            st.error(f"⚠️ {product_name} - {risk_level} risk - {risk_reason} (Potential loss: €{potential_loss:.2f})")
                else:
                    st.info("📊 Risk analysis data available but no displayable columns found")
                    
            else:
                st.info("📊 No risk analysis data available. This will be populated when inventory risk conditions are detected.")
                
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Failed to load risk analysis: {e}")
            st.error("❌ Unable to load risk analysis data")
    
    def _show_client_insights(self):
        """Show client insights."""
        st.subheader("👥 Client Insights & Loyalty")
        
        try:
            # Get clients data
            clients = self.get_mongo_data('clients')
            
            if not clients.empty:
                # Show loyalty distribution
                st.markdown("**🏆 Loyalty Tier Distribution**")
                
                loyalty_counts = clients['loyalty_tier'].value_counts()
                st.bar_chart(loyalty_counts)
                
                # Show top clients
                st.markdown("**💰 Top Clients by Revenue**")
                top_clients = clients.nlargest(5, 'total_revenue_generated')
                
                st.dataframe(
                    top_clients[['client_id', 'name', 'total_revenue_generated', 'loyalty_tier']],
                    use_container_width=True,
                    hide_index=True
                )
                
            else:
                st.info("📊 No client data available. Add clients and sales to see insights.")
                
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Failed to load client insights: {e}")
            st.error("❌ Unable to load client insights")
    
    def shutdown(self):
        """Cleanly shutdown the enhanced admin interface."""
        try:
            # Stop all background processes
            self.stop_auto_analysis()
            self.stop_kafka_transfer()
            
            # Clean up resources
            if self.mongo_client:
                self.mongo_client.close()
                self.mongo_client = None
            
            if self.mongodb_manager:
                self.mongodb_manager.shutdown()
                self.mongodb_manager = None
            
            logger.info("✅ Enhanced Admin Interface shutdown completed")
            
        except Exception as e:
            logger.error(f"❌ Enhanced Admin: Error during shutdown: {e}")

# Example usage
if __name__ == "__main__":
    from utils.config import Config
    
    # Initialize
    config = Config()
    admin = EnhancedAdminInterface(config)
    
    # Run the interface
    try:
        admin.run()
    finally:
        admin.shutdown()