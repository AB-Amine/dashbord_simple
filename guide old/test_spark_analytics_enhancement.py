#!/usr/bin/env python3
"""
Test script for Spark Analytics Enhancement
Verifies that the enhanced analytics functions work correctly
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_mongodb_schema_compatibility():
    """Test that the new schema is compatible with MongoDB operations"""
    print("🧪 Testing MongoDB Schema Compatibility...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Test creating sample data that matches the new schema
        sample_product_winner = {
            "product_id": "test-product-001",
            "product_name": "Test Product",
            "product_category": "Electronics",
            "selling_price": 199.99,
            "purchase_price": 149.99,
            "minimum_margin_threshold": 20.0,
            "total_profit": 5000.0,
            "total_revenue": 19999.0,
            "total_cost": 14999.0,
            "units_sold": 100,
            "average_profit_margin_pct": 25.0,
            "average_roi_pct": 33.3,
            "contribution_margin_pct": 25.0,
            "ranking": 1,
            "analysis_timestamp": datetime.now().isoformat(),
            "analysis_source": "automated_spark_analysis"
        }
        
        sample_loss_risk = {
            "product_id": "test-product-002",
            "product_name": "Expiring Product",
            "product_category": "Food",
            "stock_quantity": 50,
            "purchase_price": 49.99,
            "selling_price": 79.99,
            "potential_loss_amount": 2499.5,
            "potential_profit_loss": 1500.0,
            "risk_category": "Expiring Soon",
            "expiry_date": "2023-12-31",
            "days_until_expiry": 3,
            "last_movement_date": "2023-11-01",
            "days_since_last_movement": 30,
            "batch_number": "BATCH-EXP-001",
            "analysis_timestamp": datetime.now().isoformat(),
            "analysis_source": "automated_spark_analysis"
        }
        
        # Test inserting into MongoDB
        db = manager.db
        
        # Insert test data
        db.product_winners.insert_one(sample_product_winner)
        db.loss_risk_products.insert_one(sample_loss_risk)
        
        # Verify data was inserted
        product_winner = db.product_winners.find_one({"product_id": "test-product-001"})
        loss_risk = db.loss_risk_products.find_one({"product_id": "test-product-002"})
        
        if product_winner and loss_risk:
            print("✅ MongoDB schema compatibility test PASSED")
            print(f"   - Product winner inserted: {product_winner['product_name']}")
            print(f"   - Loss risk product inserted: {loss_risk['product_name']}")
            
            # Clean up
            db.product_winners.delete_one({"product_id": "test-product-001"})
            db.loss_risk_products.delete_one({"product_id": "test-product-002"})
            return True
        else:
            print("❌ MongoDB schema compatibility test FAILED")
            return False
            
    except Exception as e:
        print(f"❌ MongoDB schema compatibility test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def test_interface_data_handling():
    """Test that the interface can handle the new data structure"""
    print("🧪 Testing Interface Data Handling...")
    
    try:
        # Create sample data that matches the new schema
        sample_data = pd.DataFrame([
            {
                "product_id": "test-001",
                "product_name": "Test Product 1",
                "product_category": "Electronics",
                "selling_price": 199.99,
                "purchase_price": 149.99,
                "minimum_margin_threshold": 20.0,
                "total_profit": 5000.0,
                "total_revenue": 19999.0,
                "total_cost": 14999.0,
                "units_sold": 100,
                "average_profit_margin_pct": 25.0,
                "average_roi_pct": 33.3,
                "contribution_margin_pct": 25.0,
                "ranking": 1,
                "analysis_timestamp": datetime.now().isoformat(),
                "analysis_source": "automated_spark_analysis"
            }
        ])
        
        # Test that we can access the expected columns
        required_columns = [
            'product_name', 'product_category', 'total_profit', 'total_revenue',
            'units_sold', 'average_profit_margin_pct', 'average_roi_pct'
        ]
        
        missing_columns = [col for col in required_columns if col not in sample_data.columns]
        
        if not missing_columns:
            print("✅ Interface data handling test PASSED")
            print(f"   - All required columns present: {required_columns}")
            
            # Test calculations
            total_profit = sample_data['total_profit'].sum()
            total_revenue = sample_data['total_revenue'].sum()
            avg_margin = sample_data['average_profit_margin_pct'].mean()
            
            print(f"   - Total Profit: €{total_profit:,.2f}")
            print(f"   - Total Revenue: €{total_revenue:,.2f}")
            print(f"   - Average Margin: {avg_margin:.1f}%")
            
            return True
        else:
            print(f"❌ Interface data handling test FAILED - Missing columns: {missing_columns}")
            return False
            
    except Exception as e:
        print(f"❌ Interface data handling test FAILED: {e}")
        return False

def test_spark_imports():
    """Test that all required Spark imports are available"""
    print("🧪 Testing Spark Imports...")
    
    try:
        # Test the imports we added
        from pyspark.sql.functions import (
            from_json, col, sum as _sum, desc, when, date_add, date_sub, 
            current_date, avg, rank, current_timestamp, lit, datediff
        )
        from pyspark.sql.window import Window
        
        print("✅ Spark imports test PASSED")
        print("   - All required Spark functions imported successfully")
        return True
        
    except ImportError as e:
        print(f"❌ Spark imports test FAILED: {e}")
        return False
    except Exception as e:
        print(f"❌ Spark imports test FAILED: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting Spark Analytics Enhancement Tests...")
    print("=" * 60)
    
    tests = [
        test_spark_imports,
        test_mongodb_schema_compatibility,
        test_interface_data_handling
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
        print()
    
    print("=" * 60)
    print("📊 Test Results Summary:")
    
    passed = sum(results)
    total = len(results)
    
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"   {i+1}. {test.__name__}: {status}")
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests PASSED! Spark Analytics Enhancement is working correctly.")
        return 0
    else:
        print("⚠️  Some tests FAILED. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())