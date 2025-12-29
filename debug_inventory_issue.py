#!/usr/bin/env python3
"""
Debug script to identify the inventory data issue.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager
from pymongo import MongoClient

def debug_inventory_data():
    """Debug the inventory data structure."""
    print("🔍 Debugging Inventory Data Structure...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Get raw inventory data
        print("\n📊 Raw inventory data from MongoDB:")
        raw_inventory = manager.get_data_for_kafka('inventory')
        print(f"Found {len(raw_inventory)} inventory items")
        
        if raw_inventory:
            print("\n📋 Sample inventory item:")
            sample = raw_inventory[0]
            for key, value in sample.items():
                print(f"  {key}: {value}")
        
        # Get Kafka-formatted inventory data
        print("\n📊 Kafka-formatted inventory data:")
        kafka_inventory = manager.get_inventory_for_kafka()
        print(f"Found {len(kafka_inventory)} Kafka inventory items")
        
        if kafka_inventory:
            print("\n📋 Sample Kafka inventory item:")
            sample = kafka_inventory[0]
            for key, value in sample.items():
                print(f"  {key}: {value}")
        
        # Check for missing quantity field
        print("\n🔍 Checking for missing fields...")
        missing_quantity = [item for item in raw_inventory if 'quantity' not in item]
        print(f"Items missing 'quantity': {len(missing_quantity)}")
        
        if missing_quantity:
            print("❌ Found inventory items without 'quantity' field!")
            print("Sample item without quantity:")
            print(missing_quantity[0])
        else:
            print("✅ All inventory items have 'quantity' field")
        
        # Check MongoDB directly
        print("\n🔍 Checking MongoDB directly...")
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        direct_inventory = list(db.inventory.find({}, {'_id': 0}))
        print(f"Direct MongoDB query found {len(direct_inventory)} items")
        
        if direct_inventory:
            print("Sample direct item:")
            sample = direct_inventory[0]
            for key, value in sample.items():
                print(f"  {key}: {value}")
        
        manager.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def debug_product_winners():
    """Debug the product winners data structure."""
    print("\n🔍 Debugging Product Winners Data Structure...")
    
    try:
        config = Config()
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        # Check product_winners collection
        product_winners = list(db.product_winners.find({}, {'_id': 0}))
        print(f"Found {len(product_winners)} product winners")
        
        if product_winners:
            print("Sample product winner:")
            sample = product_winners[0]
            for key, value in sample.items():
                print(f"  {key}: {value}")
            
            # Check for required columns
            required_columns = ['product_name', 'category', 'total_sales', 'total_revenue', 'profit_margin']
            missing_columns = [col for col in required_columns if col not in sample]
            
            if missing_columns:
                print(f"❌ Missing required columns: {missing_columns}")
            else:
                print("✅ All required columns present")
        else:
            print("⚠️  No product winners found")
        
        return True
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        return False

def main():
    """Run all debug checks."""
    print("🚀 Debugging Pipeline Issues")
    print("=" * 50)
    
    # Debug inventory data
    inventory_ok = debug_inventory_data()
    
    # Debug product winners
    winners_ok = debug_product_winners()
    
    print("\n" + "=" * 50)
    print("📊 DEBUG SUMMARY:")
    print(f"  Inventory Data: {'✅ OK' if inventory_ok else '❌ ISSUE'}")
    print(f"  Product Winners: {'✅ OK' if winners_ok else '❌ ISSUE'}")

if __name__ == "__main__":
    main()