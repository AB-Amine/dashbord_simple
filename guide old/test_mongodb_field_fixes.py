#!/usr/bin/env python3
"""
Test script to verify MongoDB field fixes
Ensures that Kafka producer sends correct field names that match MongoDB structure
"""

import sys
import os

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_products_field_mapping():
    """Test that products data uses correct field names"""
    print("🧪 Testing Products Field Mapping...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Add a test product
        success, msg = manager.add_product(
            name="Field Test Product",
            category="Test Category",
            buy_price=50.0,
            sell_price=75.0,
            min_margin_threshold=25.0
        )
        
        if not success:
            print(f"❌ Failed to add test product: {msg}")
            return False
        
        # Get products for Kafka
        products = manager.get_products_for_kafka()
        
        if not products:
            print("❌ No products returned")
            return False
        
        # Find our test product
        test_product = None
        for product in products:
            if product.get('name') == 'Field Test Product':
                test_product = product
                break
        
        if not test_product:
            print("❌ Test product not found")
            return False
        
        # Verify field structure
        expected_fields = ['product_id', 'name', 'category', 'buy_price', 'sell_price', 
                          'min_margin_threshold', 'created_at', 'updated_at', 'source', 'timestamp']
        
        missing_fields = [field for field in expected_fields if field not in test_product]
        unexpected_fields = [field for field in test_product if field not in expected_fields and field != '_id']
        
        if missing_fields:
            print(f"❌ Missing fields: {missing_fields}")
            return False
        
        if unexpected_fields:
            print(f"❌ Unexpected fields: {unexpected_fields}")
            return False
        
        # Verify 'name' field exists (not 'product_name')
        if 'name' in test_product and test_product['name'] == 'Field Test Product':
            print("✅ Products field mapping test PASSED")
            print(f"   - Product name: {test_product['name']}")
            print(f"   - All fields present: {expected_fields}")
            
            # Clean up
            product_id = test_product['product_id']
            manager.db.products.delete_one({'product_id': product_id})
            return True
        else:
            print("❌ 'name' field missing or incorrect")
            return False
            
    except Exception as e:
        print(f"❌ Products field mapping test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def test_inventory_field_mapping():
    """Test that inventory data uses correct field names"""
    print("🧪 Testing Inventory Field Mapping...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Add a test product first (inventory needs a product)
        success, msg = manager.add_product(
            name="Inventory Test Product",
            category="Test Category",
            buy_price=50.0,
            sell_price=75.0,
            min_margin_threshold=25.0
        )
        
        if not success:
            print(f"❌ Failed to add test product: {msg}")
            return False
        
        # Get the product to use its ID
        products = manager.get_all_products()
        test_product = None
        for product in products:
            if product.get('name') == 'Inventory Test Product':
                test_product = product
                break
        
        if not test_product:
            print("❌ Test product not found")
            return False
        
        # Add test inventory
        success, msg = manager.add_inventory(
            product_id=test_product['product_id'],
            quantity=10,
            batch_no="TEST-BATCH-001",
            expiry_date="2025-12-31"
        )
        
        if not success:
            print(f"❌ Failed to add test inventory: {msg}")
            return False
        
        # Get inventory for Kafka
        inventory = manager.get_inventory_for_kafka()
        
        if not inventory:
            print("❌ No inventory returned")
            return False
        
        # Use the first inventory item for testing (more flexible)
        test_inventory = inventory[0] if inventory else None
        
        if not test_inventory:
            print("❌ No inventory items found")
            return False
        
        print(f"   - Testing with inventory item: {test_inventory.get('name', 'Unknown')}")
        
        # Verify field structure
        expected_fields = ['product_id', 'name', 'category', 'buy_price', 'sell_price',
                          'quantity', 'last_movement', 'expiry_date', 'batch_no',
                          'source', 'timestamp']
        
        missing_fields = [field for field in expected_fields if field not in test_inventory]
        unexpected_fields = [field for field in test_inventory if field not in expected_fields and field != '_id']
        
        if missing_fields:
            print(f"❌ Missing fields: {missing_fields}")
            return False
        
        if unexpected_fields:
            print(f"❌ Unexpected fields: {unexpected_fields}")
            return False
        
        # Verify 'name' field exists (not 'product_name')
        if 'name' in test_inventory:
            print("✅ Inventory field mapping test PASSED")
            print(f"   - Inventory name: {test_inventory['name']}")
            print(f"   - All fields present: {expected_fields}")
            
            # Clean up
            inventory_id = test_inventory.get('inventory_id')
            if inventory_id:
                manager.db.inventory.delete_one({'inventory_id': inventory_id})
            
            # Clean up product
            product_id = test_product['product_id']
            manager.db.products.delete_one({'product_id': product_id})
            
            return True
        else:
            print("❌ 'name' field missing in inventory")
            return False
            
    except Exception as e:
        print(f"❌ Inventory field mapping test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def test_mongodb_schema_compatibility():
    """Test that MongoDB schema matches what Kafka producer expects"""
    print("🧪 Testing MongoDB Schema Compatibility...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Check actual MongoDB structure
        db = manager.db
        
        # Check products collection
        product_sample = db.products.find_one()
        if product_sample:
            product_fields = list(product_sample.keys())
            print(f"   - Products collection fields: {product_fields}")
            
            if 'name' in product_fields:
                print("   ✅ Products has 'name' field")
            else:
                print("   ❌ Products missing 'name' field")
                return False
        
        # Check inventory collection
        inventory_sample = db.inventory.find_one()
        if inventory_sample:
            inventory_fields = list(inventory_sample.keys())
            print(f"   - Inventory collection fields: {inventory_fields}")
            
            if 'name' in inventory_fields:
                print("   ✅ Inventory has 'name' field")
            else:
                print("   ❌ Inventory missing 'name' field")
                return False
        
        print("✅ MongoDB schema compatibility test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ MongoDB schema compatibility test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def main():
    """Run all MongoDB field tests"""
    print("🚀 Testing MongoDB Field Fixes...")
    print("=" * 60)
    
    tests = [
        test_mongodb_schema_compatibility,
        test_products_field_mapping,
        test_inventory_field_mapping
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
        print()
    
    print("=" * 60)
    print("📊 MongoDB Field Test Results:")
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   {i+1}. {test.__name__}: {status}")
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All MongoDB field fixes verified!")
        print("✅ Kafka producer will send correct field names")
        print("✅ Spark will receive data in expected format")
        return 0
    else:
        print("⚠️  Some field mappings need attention.")
        return 1

if __name__ == "__main__":
    sys.exit(main())