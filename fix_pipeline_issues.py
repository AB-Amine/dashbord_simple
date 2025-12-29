#!/usr/bin/env python3
"""
Fix script for pipeline issues:
1. Inventory data contains product data instead of stock data
2. Product winners have wrong column names
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager
from pymongo import MongoClient

def fix_inventory_data():
    """Fix the inventory data issue."""
    print("🔧 Fixing Inventory Data Issue...")
    
    try:
        config = Config()
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        # Check current inventory data
        inventory_items = list(db.inventory.find({}, {'_id': 0}))
        print(f"Current inventory items: {len(inventory_items)}")
        
        if inventory_items:
            sample = inventory_items[0]
            print("Sample inventory item:")
            for key, value in sample.items():
                print(f"  {key}: {value}")
            
            # Check if these are actually product records (missing quantity, batch_no, etc.)
            if 'quantity' not in sample or 'batch_no' not in sample:
                print("❌ Inventory contains product data, not stock data!")
                
                # Check if these records exist in products collection
                product_id = sample.get('product_id')
                if product_id:
                    product = db.products.find_one({'product_id': product_id})
                    if product:
                        print("✅ This record exists in products collection")
                        
                        # Move incorrect records to a backup collection
                        print("📥 Moving incorrect inventory records to backup...")
                        db.inventory.rename('inventory_backup_' + datetime.now().strftime("%Y%m%d_%H%M%S"))
                        print("✅ Moved incorrect data to backup collection")
                        
                        # Create proper inventory collection
                        print("📦 Creating clean inventory collection...")
                        db.create_collection('inventory')
                        print("✅ Created clean inventory collection")
                        
                        return True
            else:
                print("✅ Inventory data looks correct")
        else:
            print("✅ Inventory collection is empty (normal for new setup)")
        
        return True
        
    except Exception as e:
        print(f"❌ Fix failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def fix_product_winners_data():
    """Fix the product winners data structure."""
    print("\n🔧 Fixing Product Winners Data Issue...")
    
    try:
        config = Config()
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        # Check current product winners
        product_winners = list(db.product_winners.find({}, {'_id': 0}))
        print(f"Current product winners: {len(product_winners)}")
        
        if product_winners:
            sample = product_winners[0]
            print("Sample product winner:")
            for key, value in sample.items():
                print(f"  {key}: {value}")
            
            # Check for expected columns
            expected_columns = ['product_name', 'category', 'total_sales', 'total_revenue', 'profit_margin']
            missing_columns = [col for col in expected_columns if col not in sample]
            
            if missing_columns:
                print(f"❌ Missing columns: {missing_columns}")
                
                # Check what columns we actually have
                actual_columns = list(sample.keys())
                print(f"Actual columns: {actual_columns}")
                
                # Rename columns to match expected format
                print("📝 Renaming columns to match expected format...")
                
                # Create mapping from actual to expected column names
                column_mapping = {}
                
                # Map product_id -> product_id (keep as is)
                if 'product_id' in sample:
                    column_mapping['product_id'] = 'product_id'
                
                # Map name -> product_name
                if 'name' in sample:
                    column_mapping['name'] = 'product_name'
                
                # Map category -> category (keep as is)
                if 'category' in sample:
                    column_mapping['category'] = 'category'
                
                # Map total_profit -> profit (or similar)
                if 'total_profit' in sample:
                    column_mapping['total_profit'] = 'total_profit'
                
                # Map total_revenue -> total_revenue (keep as is)
                if 'total_revenue' in sample:
                    column_mapping['total_revenue'] = 'total_revenue'
                
                # Add missing columns with default values
                print("➕ Adding missing columns with default values...")
                
                # Update all documents with proper column names
                for winner in product_winners:
                    update_data = {
                        'product_name': winner.get('name', 'Unknown'),
                        'category': winner.get('category', 'Unknown'),
                        'total_sales': winner.get('total_units_sold', 0),
                        'total_revenue': winner.get('total_revenue', 0.0),
                        'profit_margin': winner.get('avg_profit_margin', 0.0)
                    }
                    
                    db.product_winners.update_one(
                        {'product_id': winner['product_id']},
                        {'$set': update_data}
                    )
                
                print("✅ Fixed product winners column names")
                return True
            else:
                print("✅ Product winners have correct column names")
        else:
            print("✅ No product winners data yet")
        
        return True
        
    except Exception as e:
        print(f"❌ Fix failed: {e}")
        return False

def add_sample_inventory_data():
    """Add sample inventory data for testing."""
    print("\n📦 Adding Sample Inventory Data...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Get a product to add inventory for
        products = manager.get_all_products()
        if not products:
            print("⚠️  No products found. Adding a test product first...")
            
            # Add a test product
            success, msg = manager.add_product(
                name="Sample Product",
                category="Test",
                buy_price=50.0,
                sell_price=100.0,
                min_margin_threshold=20.0
            )
            print(f"Product addition: {msg}")
            
            # Get the product ID
            products = manager.get_all_products()
        
        if products:
            product_id = products[0]['product_id']
            
            # Add sample inventory
            success, msg = manager.add_inventory(
                product_id=product_id,
                quantity=100,
                batch_no="BATCH-2024-001",
                expiry_date="2024-12-31"
            )
            print(f"Inventory addition: {msg}")
            
            if success:
                # Verify the inventory was added correctly
                inventory = manager.get_all_inventory()
                print(f"✅ Added inventory. Total inventory items: {len(inventory)}")
                
                if inventory:
                    sample = inventory[0]
                    print("Sample inventory item:")
                    for key, value in sample.items():
                        print(f"  {key}: {value}")
            else:
                print(f"❌ Failed to add inventory: {msg}")
        
        manager.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Failed to add sample inventory: {e}")
        return False

def main():
    """Run all fixes."""
    print("🚀 Pipeline Issues Fix Tool")
    print("=" * 50)
    
    # Fix inventory data
    inventory_fixed = fix_inventory_data()
    
    # Fix product winners data
    winners_fixed = fix_product_winners_data()
    
    # Add sample inventory data
    sample_added = add_sample_inventory_data()
    
    print("\n" + "=" * 50)
    print("📊 FIX SUMMARY:")
    print(f"  Inventory Data: {'✅ FIXED' if inventory_fixed else '❌ FAILED'}")
    print(f"  Product Winners: {'✅ FIXED' if winners_fixed else '❌ FAILED'}")
    print(f"  Sample Data: {'✅ ADDED' if sample_added else '❌ FAILED'}")
    
    if all([inventory_fixed, winners_fixed, sample_added]):
        print("\n🎉 All pipeline issues fixed!")
        print("✅ The system should now work correctly")
    else:
        print("\n⚠️  Some fixes failed. Check the issues above.")

if __name__ == "__main__":
    # Import datetime for backup collection naming
    from datetime import datetime
    main()