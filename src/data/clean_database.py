#!/usr/bin/env python3
"""
Database Cleanup Script
Drops all existing data and creates a clean database structure
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from utils.config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DatabaseCleanup")

class DatabaseCleanup:
    def __init__(self):
        """Initialize database cleanup with configuration."""
        self.config = Config()
        self.client = None
        self.db = None
        
    def connect_to_mongodb(self):
        """Connect to MongoDB database."""
        try:
            self.client = MongoClient(self.config.MONGO_URI)
            self.db = self.client[self.config.DB_NAME]
            logger.info(f"✅ Connected to MongoDB: {self.config.MONGO_URI}")
            logger.info(f"🗄️  Using database: {self.config.DB_NAME}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False
    
    def drop_all_collections(self):
        """Drop all collections in the database."""
        try:
            if self.db is None:
                logger.error("❌ Database connection not established")
                return False
            
            # Get all collection names
            collection_names = self.db.list_collection_names()
            
            if not collection_names:
                logger.info("ℹ️  No collections to drop - database is already empty")
                return True
            
            logger.info(f"🗑️  Dropping {len(collection_names)} collections...")
            
            # Drop each collection
            for collection_name in collection_names:
                self.db.drop_collection(collection_name)
                logger.info(f"🗑️  Dropped collection: {collection_name}")
            
            logger.info("✅ All collections dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to drop collections: {e}")
            return False
    
    def create_indexes(self):
        """Create indexes for better query performance."""
        try:
            if self.db is None:
                logger.error("❌ Database connection not established")
                return False
            
            logger.info("🔧 Creating indexes for optimal performance...")
            
            # Products collection indexes
            self.db.products.create_index([('name', 1)])
            self.db.products.create_index([('category', 1)])
            self.db.products.create_index([('product_id', 1)], unique=True)
            
            # Clients collection indexes
            self.db.clients.create_index([('name', 1)])
            self.db.clients.create_index([('email', 1)])
            self.db.clients.create_index([('loyalty_tier', 1)])
            self.db.clients.create_index([('client_id', 1)], unique=True)
            
            # Sales collection indexes
            self.db.sales.create_index([('client_id', 1)])
            self.db.sales.create_index([('timestamp', -1)])
            self.db.sales.create_index([('invoice_id', 1)], unique=True)
            
            # Inventory collection indexes
            self.db.inventory.create_index([('product_id', 1)])
            self.db.inventory.create_index([('batch_no', 1)])
            self.db.inventory.create_index([('expiry_date', 1)])
            self.db.inventory.create_index([('inventory_id', 1)], unique=True)
            
            # Analytics collections indexes
            self.db.product_winners.create_index([('product_id', 1)])
            self.db.product_winners.create_index([('total_profit', -1)])
            
            self.db.loss_risk_products.create_index([('product_id', 1)])
            self.db.loss_risk_products.create_index([('risk_level', 1)])
            
            logger.info("✅ All indexes created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create indexes: {e}")
            return False
    
    def verify_database_state(self):
        """Verify the database is clean and ready."""
        try:
            if self.db is None:
                logger.error("❌ Database connection not established")
                return False
            
            collection_names = self.db.list_collection_names()
            
            # Check if we have the expected empty collections (created by indexes)
            expected_collections = ['products', 'clients', 'sales', 'inventory', 'product_winners', 'loss_risk_products']
            
            # If we have exactly the expected collections and they're all empty, that's good
            if set(collection_names) == set(expected_collections):
                # Check if collections are empty
                all_empty = True
                for collection_name in collection_names:
                    count = self.db[collection_name].count_documents({})
                    if count > 0:
                        all_empty = False
                        logger.warning(f"⚠️  Collection {collection_name} has {count} documents")
                        break
                
                if all_empty:
                    logger.info("✅ Database is clean and ready with proper structure")
                    return True
                else:
                    logger.warning("⚠️  Some collections still contain data")
                    return False
            else:
                logger.warning(f"⚠️  Unexpected collections found: {collection_names}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Failed to verify database state: {e}")
            return False
    
    def cleanup(self):
        """Clean up database connections."""
        try:
            if self.client:
                self.client.close()
                logger.info("✅ MongoDB connection closed")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
    
    def run(self):
        """Run the complete database cleanup process."""
        logger.info("🚀 Starting database cleanup process...")
        
        try:
            # Step 1: Connect to MongoDB
            if not self.connect_to_mongodb():
                return False
            
            # Step 2: Drop all collections
            if not self.drop_all_collections():
                return False
            
            # Step 3: Create indexes
            if not self.create_indexes():
                return False
            
            # Step 4: Verify database state
            if not self.verify_database_state():
                return False
            
            logger.info("🎉 Database cleanup completed successfully!")
            logger.info("📊 Database is now clean and ready for new data")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database cleanup failed: {e}")
            return False
        finally:
            self.cleanup()

def main():
    """Main entry point for database cleanup."""
    cleanup = DatabaseCleanup()
    success = cleanup.run()
    
    if success:
        logger.info("✅ You can now populate the database with fresh data")
        return 0
    else:
        logger.error("❌ Database cleanup failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())