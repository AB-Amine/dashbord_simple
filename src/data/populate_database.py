#!/usr/bin/env python3
"""
Database Population Script
Populates the clean database with sample data to establish proper data structures
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from utils.config import Config
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DatabasePopulation")

class DatabasePopulation:
    def __init__(self):
        """Initialize database population with configuration."""
        self.config = Config()
        self.manager = MongoDBAccountantManager(self.config)
        
    def populate_products(self):
        """Populate products collection with sample data."""
        logger.info("📦 Populating products collection...")
        
        products = [
            {
                'name': 'Organic Apples',
                'category': 'Produce',
                'buy_price': 1.50,
                'sell_price': 2.99,
                'min_margin_threshold': 30.0
            },
            {
                'name': 'Free Range Eggs',
                'category': 'Dairy',
                'buy_price': 3.00,
                'sell_price': 5.99,
                'min_margin_threshold': 25.0
            },
            {
                'name': 'Whole Grain Bread',
                'category': 'Bakery',
                'buy_price': 2.50,
                'sell_price': 4.99,
                'min_margin_threshold': 35.0
            },
            {
                'name': 'Almond Milk',
                'category': 'Beverages',
                'buy_price': 2.00,
                'sell_price': 3.99,
                'min_margin_threshold': 28.0
            },
            {
                'name': 'Grass-Fed Beef',
                'category': 'Meat',
                'buy_price': 12.00,
                'sell_price': 19.99,
                'min_margin_threshold': 22.0
            }
        ]
        
        success_count = 0
        for product in products:
            success, msg = self.manager.add_product(
                product['name'],
                product['category'],
                product['buy_price'],
                product['sell_price'],
                product['min_margin_threshold']
            )
            if success:
                success_count += 1
                logger.info(f"✅ Added product: {product['name']}")
            else:
                logger.error(f"❌ Failed to add product {product['name']}: {msg}")
        
        logger.info(f"📊 Added {success_count}/{len(products)} products")
        return success_count > 0
    
    def populate_clients(self):
        """Populate clients collection with sample data."""
        logger.info("👥 Populating clients collection...")
        
        clients = [
            {
                'name': 'Green Grocer Market',
                'email': 'contact@greengrocer.com',
                'phone': '+1-555-123-4567'
            },
            {
                'name': 'Healthy Harvest Co-op',
                'email': 'info@healthyharvest.com',
                'phone': '+1-555-234-5678'
            },
            {
                'name': 'Farm Fresh Foods',
                'email': 'sales@farmfresh.com',
                'phone': '+1-555-345-6789'
            },
            {
                'name': 'Organic Oasis',
                'email': 'hello@organicoasis.com',
                'phone': '+1-555-456-7890'
            },
            {
                'name': 'Wholesome Pantry',
                'email': 'support@wholesomepantry.com',
                'phone': '+1-555-567-8901'
            }
        ]
        
        success_count = 0
        for client in clients:
            success, msg = self.manager.add_client(
                client['name'],
                client['email'],
                client['phone']
            )
            if success:
                success_count += 1
                logger.info(f"✅ Added client: {client['name']}")
            else:
                logger.error(f"❌ Failed to add client {client['name']}: {msg}")
        
        logger.info(f"📊 Added {success_count}/{len(clients)} clients")
        return success_count > 0
    
    def populate_inventory(self):
        """Populate inventory collection with sample data."""
        logger.info("📊 Populating inventory collection...")
        
        # Get all products first
        products = self.manager.get_all_products()
        if not products:
            logger.error("❌ No products available to create inventory")
            return False
        
        inventory_items = []
        today = datetime.now()
        
        for i, product in enumerate(products):
            # Create multiple batches for each product
            for batch_num in range(1, 4):  # 3 batches per product
                expiry_date = (today + timedelta(days=30 + (i * 10))).strftime('%Y-%m-%d')
                inventory_items.append({
                    'product_id': product['product_id'],
                    'quantity': 50 + (i * 10),
                    'batch_no': f'BATCH-{product['name'][:3].upper()}-{batch_num:03d}',
                    'expiry_date': expiry_date,
                    'location': 'Main Warehouse'
                })
        
        success_count = 0
        for item in inventory_items:
            success, msg = self.manager.add_inventory(
                item['product_id'],
                item['quantity'],
                item['batch_no'],
                item['expiry_date'],
                item['location']
            )
            if success:
                success_count += 1
                logger.info(f"✅ Added inventory: {item['batch_no']}")
            else:
                logger.error(f"❌ Failed to add inventory {item['batch_no']}: {msg}")
        
        logger.info(f"📊 Added {success_count}/{len(inventory_items)} inventory items")
        return success_count > 0
    
    def populate_sales(self):
        """Populate sales collection with sample data."""
        logger.info("💰 Populating sales collection...")
        
        # Get all clients and products
        clients = self.manager.get_all_clients()
        products = self.manager.get_all_products()
        
        if not clients or not products:
            logger.error("❌ No clients or products available to create sales")
            return False
        
        # Create sample sales
        sales_data = []
        today = datetime.now()
        
        for sale_num in range(1, 11):  # 10 sales
            client = clients[sale_num % len(clients)]
            
            # Each sale has 1-3 items
            num_items = min(3, sale_num % 3 + 1)
            items = []
            
            for item_num in range(num_items):
                product = products[(sale_num + item_num) % len(products)]
                quantity = 5 + (sale_num % 10)
                items.append({
                    'product_id': product['product_id'],
                    'quantity': quantity
                })
            
            sales_data.append({
                'client_id': client['client_id'],
                'items': items
            })
        
        success_count = 0
        for sale in sales_data:
            success, msg = self.manager.add_sale(
                sale['client_id'],
                sale['items']
            )
            if success:
                success_count += 1
                logger.info(f"✅ Added sale for client: {sale['client_id']}")
            else:
                logger.error(f"❌ Failed to add sale: {msg}")
        
        logger.info(f"📊 Added {success_count}/{len(sales_data)} sales")
        return success_count > 0
    
    def verify_data_structure(self):
        """Verify that all collections have the proper data structure."""
        logger.info("🔍 Verifying data structure...")
        
        try:
            # Check each collection
            collections_to_check = [
                ('products', self.manager.get_all_products()),
                ('clients', self.manager.get_all_clients()),
                ('inventory', self.manager.get_all_inventory()),
                ('sales', self.manager.get_all_sales())
            ]
            
            all_valid = True
            for collection_name, data in collections_to_check:
                if not data:
                    logger.warning(f"⚠️  Collection '{collection_name}' is empty")
                    all_valid = False
                else:
                    logger.info(f"✅ Collection '{collection_name}' has {len(data)} records")
                    
                    # Check first record structure
                    sample_record = data[0]
                    logger.debug(f"Sample {collection_name} record: {list(sample_record.keys())}")
            
            if all_valid:
                logger.info("✅ All data structures are valid")
                return True
            else:
                logger.error("❌ Some data structures are invalid")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to verify data structure: {e}")
            return False
    
    def run(self):
        """Run the complete database population process."""
        logger.info("🚀 Starting database population process...")
        
        try:
            # Step 1: Populate products
            if not self.populate_products():
                logger.error("❌ Failed to populate products")
                return False
            
            # Step 2: Populate clients
            if not self.populate_clients():
                logger.error("❌ Failed to populate clients")
                return False
            
            # Step 3: Populate inventory
            if not self.populate_inventory():
                logger.error("❌ Failed to populate inventory")
                return False
            
            # Step 4: Populate sales
            if not self.populate_sales():
                logger.error("❌ Failed to populate sales")
                return False
            
            # Step 5: Verify data structure
            if not self.verify_data_structure():
                logger.error("❌ Data structure verification failed")
                return False
            
            logger.info("🎉 Database population completed successfully!")
            logger.info("📊 Database is now populated with sample data")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database population failed: {e}")
            return False
        finally:
            self.manager.shutdown()

def main():
    """Main entry point for database population."""
    population = DatabasePopulation()
    success = population.run()
    
    if success:
        logger.info("✅ Database is ready for use with proper data structures")
        return 0
    else:
        logger.error("❌ Database population failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())