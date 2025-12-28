#!/usr/bin/env python3
"""
Real-time Kafka Producer for Continuous Data Import
Runs every 10 seconds to import data from MongoDB to Kafka topics
"""

import sys
import os
import time
import logging
import threading
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from kafka.kafka_producer import KafkaProducer
from data.mongodb_accountant_manager import MongoDBAccountantManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RealTimeKafkaProducer")

class RealTimeKafkaProducer:
    """
    Real-time Kafka producer that continuously imports data from MongoDB to Kafka
    every 10 seconds for Spark streaming processing.
    """
    
    def __init__(self, config: Config):
        """Initialize the real-time Kafka producer."""
        self.config = config
        self.kafka_producer = KafkaProducer(config)
        self.mongodb_manager = MongoDBAccountantManager(config)
        self.running = False
        self.interval = 10  # seconds
        
    def import_data_to_kafka(self):
        """Import data from MongoDB to Kafka topics."""
        try:
            logger.info("🎯 Starting real-time data import to Kafka...")
            
            # Get data from MongoDB
            products = self.mongodb_manager.get_products_for_kafka()
            clients = self.mongodb_manager.get_clients_for_kafka()
            sales = self.mongodb_manager.get_sales_for_kafka()
            inventory = self.mongodb_manager.get_inventory_for_kafka()
            
            # Transfer data to Kafka
            success_count = 0
            
            if products:
                if self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    products
                ):
                    success_count += 1
                    logger.info(f"✅ Products: {len(products)} records sent to Kafka")
            
            if clients:
                if self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('clients'), 
                    clients
                ):
                    success_count += 1
                    logger.info(f"✅ Clients: {len(clients)} records sent to Kafka")
            
            if sales:
                if self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('sales'), 
                    sales
                ):
                    success_count += 1
                    logger.info(f"✅ Sales: {len(sales)} records sent to Kafka")
            
            if inventory:
                if self.kafka_producer.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    inventory
                ):
                    success_count += 1
                    logger.info(f"✅ Inventory: {len(inventory)} records sent to Kafka")
            
            if success_count > 0:
                logger.info(f"🎉 Real-time data import completed: {success_count}/4 topics updated")
                return True
            else:
                logger.warning("⚠️  No data to import or all imports failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Real-time data import failed: {e}")
            return False
    
    def run_continuous_import(self):
        """Run continuous data import every 10 seconds."""
        self.running = True
        import_count = 0
        
        try:
            logger.info(f"🚀 Starting continuous Kafka data import (every {self.interval} seconds)")
            logger.info("📡 Press Ctrl+C to stop...")
            
            while self.running:
                start_time = time.time()
                
                try:
                    success = self.import_data_to_kafka()
                    if success:
                        import_count += 1
                        
                    # Calculate sleep time to maintain 10-second interval
                    processing_time = time.time() - start_time
                    sleep_time = max(0, self.interval - processing_time)
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                        
                except KeyboardInterrupt:
                    logger.info("🛑 Received keyboard interrupt, stopping...")
                    self.running = False
                    break
                except Exception as e:
                    logger.error(f"❌ Error in continuous import loop: {e}")
                    # Wait before retrying
                    time.sleep(min(5, self.interval))
            
            logger.info(f"✅ Continuous import completed: {import_count} successful imports")
            
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Cleanly shutdown the real-time producer."""
        try:
            self.running = False
            logger.info("🛑 Shutting down real-time Kafka producer...")
            
            if self.kafka_producer:
                self.kafka_producer.shutdown()
            
            if self.mongodb_manager:
                self.mongodb_manager.shutdown()
                
            logger.info("✅ Real-time Kafka producer shutdown completed")
            
        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("🛑 Received shutdown signal, stopping gracefully...")
    global realtime_producer
    if realtime_producer and realtime_producer.running:
        realtime_producer.running = False
    sys.exit(0)

def main():
    """Main entry point for real-time Kafka producer."""
    import signal
    
    # Set up signal handling for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        config = Config()
        
        # Initialize and run the real-time producer
        global realtime_producer
        realtime_producer = RealTimeKafkaProducer(config)
        
        # Run continuous import
        realtime_producer.run_continuous_import()
        
    except Exception as e:
        logger.error(f"❌ Application error: {e}")
        if 'realtime_producer' in globals():
            realtime_producer.shutdown()
    finally:
        logger.info("✅ Application exit complete")

if __name__ == "__main__":
    main()