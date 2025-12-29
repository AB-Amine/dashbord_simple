#!/usr/bin/env python3
"""
Main application class for the Real-Time Economic Analytics Platform.
This class orchestrates all components of the system.
"""

import sys
import os
import threading
import logging
from typing import Optional

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interface.user_interface import UserInterface
from data.mongodb_accountant_manager import MongoDBAccountantManager
from kafka.kafka_producer import KafkaProducer
from data.historical_data_reingestion import HistoricalDataReingestion
from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealTimeAnalyticsApplication:
    """
    Main application class that orchestrates all system components.
    """
    
    def __init__(self):
        """Initialize all components of the application."""
        self.config = Config()
        self.mongodb_manager = MongoDBAccountantManager(self.config)
        self.kafka_producer = KafkaProducer(self.config)
        self.user_interface = UserInterface(self.config)
        self.running = False
        self.initialized = False  # Track initialization state to prevent duplicates
        self.historical_reingestion = HistoricalDataReingestion(self.config, self.kafka_producer)
        
    def initialize_system(self) -> bool:
        """Initialize all system components."""
        try:
            if self.initialized:
                logger.info("⚠️  System already initialized - skipping duplicate initialization")
                return True
                
            logger.info("🚀 Initializing system components...")
            
            # Initialize MongoDB (no database initialization needed - MongoDB is schema-less)
            logger.info("Initializing MongoDB Accountant Manager...")
            # MongoDBAccountantManager initializes automatically in constructor
            logger.info("✅ MongoDB Accountant Manager initialized")
            
            # Initialize Kafka topics
            logger.info("Initializing Kafka topics...")
            if not self.kafka_producer.initialize_topics():
                logger.error("Kafka topics initialization failed")
                return False
            

            
            logger.info("✅ System initialization completed successfully")
            self.initialized = True  # Mark as initialized
            return True
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}")
            return False
    
    def run(self):
        """Run the main application."""
        logger.info("🚀 Starting application run cycle...")
        
        if not self.initialize_system():
            logger.error("❌ Failed to initialize system. Exiting...")
            print("Failed to initialize system. Exiting...")
            return
        
        # Start the user interface
        logger.info("🖥️  Starting user interface...")
        self.user_interface.run()
        logger.info("📺 User interface completed")
    
    def process_data_file(self, file_path: str):
        """Process data from an Excel file through the NEW MongoDB pipeline."""
        try:
            # NEW ARCHITECTURE: Load data directly into MongoDB
            logger.info("🚀 Loading data into MongoDB (new architecture)...")
            
            # Read Excel file
            try:
                products_df = pd.read_excel(file_path, sheet_name='Products')
                clients_df = pd.read_excel(file_path, sheet_name='Clients')
                sales_df = pd.read_excel(file_path, sheet_name='Sales')
                inventory_df = pd.read_excel(file_path, sheet_name='Inventory')
                
                # Convert DataFrames to dictionaries
                products_data = products_df.to_dict('records')
                clients_data = clients_df.to_dict('records')
                sales_data = sales_df.to_dict('records')
                inventory_data = inventory_df.to_dict('records')
                
                # Add products to MongoDB
                for product in products_data:
                    success, msg = self.mongodb_manager.add_product(
                        name=product['name'],
                        category=product['category'],
                        buy_price=product['buy_price'],
                        sell_price=product['sell_price'],
                        min_margin_threshold=product.get('min_margin_threshold', 20.0)
                    )
                    if not success:
                        logger.error(f"Failed to add product: {msg}")
                        return False
                
                # Add clients to MongoDB
                for client in clients_data:
                    success, msg = self.mongodb_manager.add_client(
                        name=client['name'],
                        email=client.get('email'),
                        phone=client.get('phone')
                    )
                    if not success:
                        logger.error(f"Failed to add client: {msg}")
                        return False
                
                # Add inventory to MongoDB
                products = self.mongodb_manager.get_all_products()
                for inventory in inventory_data:
                    if products:
                        # Find matching product (simple matching by name for demo)
                        product_id = None
                        for product in products:
                            if product['name'] == inventory.get('product_name'):
                                product_id = product['product_id']
                                break
                        
                        if product_id:
                            success, msg = self.mongodb_manager.add_inventory(
                                product_id=product_id,
                                quantity=inventory['quantity'],
                                batch_no=inventory.get('batch_no', 'BATCH-001'),
                                expiry_date=inventory.get('expiry_date', '2025-12-31')
                            )
                            if not success:
                                logger.error(f"Failed to add inventory: {msg}")
                                return False
                
                # Add sales to MongoDB
                clients = self.mongodb_manager.get_all_clients()
                for sale in sales_data:
                    if clients:
                        # Find matching client (simple matching by name for demo)
                        client_id = None
                        for client in clients:
                            if client['name'] == sale.get('client_name'):
                                client_id = client['client_id']
                                break
                        
                        if client_id:
                            # Find products for sale items
                            sale_items = []
                            for product in products:
                                # Simple matching - in real app would use proper product IDs
                                if any(item.get('product_name') == product['name'] for item in sale.get('items', [])):
                                    sale_items.append({
                                        'product_id': product['product_id'],
                                        'quantity': 1  # Default quantity
                                    })
                            
                            if sale_items:
                                success, msg = self.mongodb_manager.add_sale(
                                    client_id=client_id,
                                    items=sale_items
                                )
                                if not success:
                                    logger.error(f"Failed to add sale: {msg}")
                                    return False
                
                logger.info("✅ Data successfully loaded into MongoDB")
                
            except Exception as e:
                logger.error(f"Failed to load Excel data into MongoDB: {e}")
                return False
            
            # Step 2: Transfer data from MongoDB to Kafka (NEW ARCHITECTURE)
            logger.info("🚀 Transferring data from MongoDB to Kafka...")
            if not self._transfer_mongodb_to_kafka():
                logger.error("Failed to transfer data from MongoDB to Kafka")
                return False
            
            # Step 3: Spark analytics runs as standalone process
            logger.info("🚀 Spark analytics runs as standalone process - no action needed")
            
            return True
        except Exception as e:
            logger.error(f"Data processing failed: {e}")
            return False


    
    def _transfer_mongodb_to_kafka(self) -> bool:
        """Transfer data from MongoDB to Kafka (NEW ARCHITECTURE).
        
        This method gets data from MongoDB and sends it to Kafka topics,
        replacing the old SQLite to Kafka transfer.
        
        Returns:
            bool: True if transfer was successful, False otherwise
        """
        logger.info("🎯 Starting MongoDB to Kafka data transfer...")
        
        try:
            # Get data from MongoDB using the new manager
            products = self.mongodb_manager.get_products_for_kafka()
            clients = self.mongodb_manager.get_clients_for_kafka()
            sales = self.mongodb_manager.get_sales_for_kafka()
            inventory = self.mongodb_manager.get_inventory_for_kafka()
            
            # Debug logging to show what data we have
            logger.info(f"📊 Data available for transfer: products={len(products)}, clients={len(clients)}, sales={len(sales)}, inventory={len(inventory)}")
            
            # Transfer products to inventory topic
            if products:
                logger.info(f"📦 Transferring {len(products)} products to Kafka")
                if not self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    products
                ):
                    logger.error("Failed to transfer products to Kafka")
                    return False
            
            # Transfer clients to clients topic
            if clients:
                logger.info(f"👥 Transferring {len(clients)} clients to Kafka")
                if not self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('clients'), 
                    clients
                ):
                    logger.error("Failed to transfer clients to Kafka")
                    return False
            
            # Transfer sales to sales topic
            if sales:
                logger.info(f"💰 Transferring {len(sales)} sales to Kafka")
                if not self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('sales'), 
                    sales
                ):
                    logger.error("Failed to transfer sales to Kafka")
                    return False
            
            # Transfer inventory to inventory topic
            if inventory:
                logger.info(f"📦 Transferring {len(inventory)} inventory items to Kafka")
                if not self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    inventory
                ):
                    logger.error("Failed to transfer inventory to Kafka")
                    return False
            
            logger.info("✅ MongoDB to Kafka data transfer completed successfully")
            logger.info("🚀 Spark Structured Streaming will automatically process the data")
            logger.info("📊 MongoDB analytics collections will be updated")
            
            # If we have data, ensure Spark analytics is running
            if any([products, clients, sales, inventory]):
                self._ensure_spark_analytics_running()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ MongoDB to Kafka transfer error: {e}")
            return False
    
    def _ensure_spark_analytics_running(self):
        """Ensure Spark analytics processing is running."""
        try:
            # Import Spark analytics module
            from analytics.spark_streaming_main import RealTimeAnalyticsStreaming
            
            # Create and start Spark processing in a separate thread
            spark_app = RealTimeAnalyticsStreaming()
            
            # Initialize Spark
            if spark_app.initialize_spark():
                logger.info("✅ Spark analytics session initialized")
                
                # Start processing streams in a separate thread to avoid blocking
                import threading
                spark_thread = threading.Thread(target=spark_app.process_streams, daemon=True)
                spark_thread.start()
                logger.info("🚀 Spark analytics processing started in background thread")
            else:
                logger.error("❌ Failed to initialize Spark analytics")
                
        except Exception as e:
            logger.error(f"❌ Error ensuring Spark analytics is running: {e}")
    
    def trigger_historical_reingestion(self) -> bool:
        """Trigger historical data re-ingestion.
        
        This method reads all existing data from MongoDB and publishes it to Kafka,
        allowing Spark Structured Streaming to process it automatically.
        
        Returns:
            bool: True if re-ingestion was successful, False otherwise
        """
        logger.info("🎯 Starting historical data re-ingestion process...")
        
        try:
            success = self.historical_reingestion.reingest_all_historical_data()
            if success:
                logger.info("✅ Historical data re-ingestion completed successfully")
                logger.info("🚀 Spark Structured Streaming will automatically process the historical data")
                logger.info("📊 MongoDB will be updated with historical analytics")
                return True
            else:
                logger.error("❌ Historical data re-ingestion failed")
                return False
        except Exception as e:
            logger.error(f"❌ Historical data re-ingestion error: {e}")
            return False
    
    def shutdown(self):
        """Cleanly shutdown all components."""
        try:
            logger.info("🛑 Shutting down application...")
            
            # Shutdown other components
            if self.kafka_producer:
                logger.info("📡 Shutting down Kafka producer...")
                self.kafka_producer.shutdown()
            
            if self.mongodb_manager:
                logger.info("🗄️  Shutting down MongoDB manager...")
                self.mongodb_manager.shutdown()
            
            logger.info("✅ Application shutdown completed")
            
        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")


def main():
    """Entry point for the application."""
    app = RealTimeAnalyticsApplication()
    
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("\nReceived keyboard interrupt, shutting down...")
        app.shutdown()
    except Exception as e:
        logger.error(f"Application error: {e}")
        app.shutdown()
    finally:
        logger.info("Application exit complete")


if __name__ == "__main__":
    main()