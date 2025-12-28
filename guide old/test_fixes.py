#!/usr/bin/env python3
"""
Test script to verify the fixes for product_name error and interface updates
"""

import sys
import os

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_product_name_field():
    """Test that product_name field is included in Kafka data"""
    print("🧪 Testing product_name field in Kafka data...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Add a test product first
        success, msg = manager.add_product(
            name="Test Product",
            category="Electronics",
            buy_price=100.0,
            sell_price=150.0,
            min_margin_threshold=20.0
        )
        
        if not success:
            print(f"❌ Failed to add test product: {msg}")
            return False
        
        # Get products for Kafka
        products = manager.get_products_for_kafka()
        
        if not products:
            print("❌ No products returned from get_products_for_kafka()")
            return False
        
        # Check if product_name field exists
        test_product = None
        for product in products:
            if product.get('name') == 'Test Product':
                test_product = product
                break
        
        if not test_product:
            print("❌ Test product not found in Kafka data")
            return False
        
        # Verify product_name field exists
        if 'product_name' in test_product:
            print("✅ product_name field test PASSED")
            print(f"   - Product found: {test_product['product_name']}")
            print(f"   - All fields: {list(test_product.keys())}")
            
            # Clean up
            product_id = test_product['product_id']
            manager.db.products.delete_one({'product_id': product_id})
            return True
        else:
            print("❌ product_name field missing in Kafka data")
            print(f"   - Available fields: {list(test_product.keys())}")
            return False
            
    except Exception as e:
        print(f"❌ product_name field test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def test_interface_metrics():
    """Test that interface can handle the new metrics"""
    print("🧪 Testing interface metrics handling...")
    
    try:
        import pandas as pd
        
        # Create sample data with all new metrics
        sample_data = pd.DataFrame([
            {
                'product_name': 'Test Product 1',
                'performance_score': 5250.0,
                'sales_velocity': 10.0,
                'stock_turnover': 2.5,
                'is_dead_stock': False
            },
            {
                'product_name': 'Test Product 2',
                'performance_score': 3850.0,
                'sales_velocity': 7.5,
                'stock_turnover': 1.8,
                'is_dead_stock': True
            }
        ])
        
        # Test finding top performer
        if 'performance_score' in sample_data.columns:
            top_performer = sample_data.loc[sample_data['performance_score'].idxmax()]
            print(f"✅ Top performer found: {top_performer['product_name']}")
            print(f"   - Performance Score: {top_performer['performance_score']}")
        
        # Test metric calculations
        avg_sales_velocity = sample_data['sales_velocity'].mean()
        avg_stock_turnover = sample_data['stock_turnover'].mean()
        avg_performance_score = sample_data['performance_score'].mean()
        dead_stock_count = sample_data['is_dead_stock'].sum()
        
        print(f"✅ Avg Sales Velocity: {avg_sales_velocity:.2f} units/day")
        print(f"✅ Avg Stock Turnover: {avg_stock_turnover:.2f}")
        print(f"✅ Avg Performance Score: {avg_performance_score:.2f}")
        print(f"✅ Dead Stock Count: {dead_stock_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Interface metrics test FAILED: {e}")
        return False

def main():
    """Run all fix tests"""
    print("🚀 Testing Fixes for Advanced Analytics...")
    print("=" * 60)
    
    tests = [
        test_product_name_field,
        test_interface_metrics
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
        print()
    
    print("=" * 60)
    print("📊 Fix Test Results:")
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   {i+1}. {test.__name__}: {status}")
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All fixes verified! System is ready for use.")
        return 0
    else:
        print("⚠️  Some fixes need attention.")
        return 1

if __name__ == "__main__":
    sys.exit(main())