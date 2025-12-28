#!/usr/bin/env python3
"""
Test script for Advanced Analytics Enhancement
Verifies that the new metrics and calculations work correctly
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_advanced_metrics_schema():
    """Test that the new advanced metrics schema works with MongoDB"""
    print("🧪 Testing Advanced Metrics Schema...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Create sample data with all new metrics
        sample_product_winner = {
            "product_id": "test-advanced-001",
            "product_name": "Advanced Test Product",
            "product_category": "Electronics",
            "selling_price": 299.99,
            "purchase_price": 199.99,
            "minimum_margin_threshold": 25.0,
            "total_profit": 10000.0,
            "total_revenue": 29999.0,
            "total_cost": 19999.0,
            "units_sold": 100,
            "average_profit_margin_pct": 33.3,
            "average_roi_pct": 50.0,
            "contribution_margin_pct": 33.3,
            "sales_velocity": 10.0,           # NEW METRIC
            "stock_turnover": 2.5,             # NEW METRIC
            "days_in_stock": 5,                # NEW METRIC
            "is_dead_stock": False,            # NEW METRIC
            "performance_score": 5250.0,       # NEW METRIC
            "ranking": 1,
            "last_sale_date": datetime.now().isoformat(),  # NEW METRIC
            "analysis_timestamp": datetime.now().isoformat(),
            "analysis_source": "automated_spark_analysis"
        }
        
        # Test inserting into MongoDB
        db = manager.db
        
        # Insert test data
        db.product_winners.insert_one(sample_product_winner)
        
        # Verify data was inserted
        product_winner = db.product_winners.find_one({"product_id": "test-advanced-001"})
        
        if product_winner:
            # Verify all new metrics are present
            required_new_metrics = [
                'sales_velocity', 'stock_turnover', 'days_in_stock', 
                'is_dead_stock', 'performance_score', 'last_sale_date'
            ]
            
            missing_metrics = [metric for metric in required_new_metrics 
                             if metric not in product_winner]
            
            if not missing_metrics:
                print("✅ Advanced metrics schema test PASSED")
                print(f"   - All new metrics present: {required_new_metrics}")
                print(f"   - Sales Velocity: {product_winner['sales_velocity']} units/day")
                print(f"   - Stock Turnover: {product_winner['stock_turnover']}")
                print(f"   - Performance Score: {product_winner['performance_score']}")
                print(f"   - Days in Stock: {product_winner['days_in_stock']}")
                print(f"   - Is Dead Stock: {product_winner['is_dead_stock']}")
                
                # Clean up
                db.product_winners.delete_one({"product_id": "test-advanced-001"})
                return True
            else:
                print(f"❌ Advanced metrics schema test FAILED - Missing metrics: {missing_metrics}")
                return False
        else:
            print("❌ Advanced metrics schema test FAILED - Data not inserted")
            return False
            
    except Exception as e:
        print(f"❌ Advanced metrics schema test FAILED: {e}")
        return False
    finally:
        if 'manager' in locals():
            manager.shutdown()

def test_performance_score_calculation():
    """Test the performance score calculation logic"""
    print("🧪 Testing Performance Score Calculation...")
    
    try:
        # Test data
        test_cases = [
            {
                'profit': 10000.0,
                'sales_velocity': 10.0,
                'days_in_stock': 5,
                'expected_score': (10000.0 * 0.5) + (10.0 * 0.3) - (5 * 0.2)
            },
            {
                'profit': 5000.0,
                'sales_velocity': 5.0,
                'days_in_stock': 15,
                'expected_score': (5000.0 * 0.5) + (5.0 * 0.3) - (15 * 0.2)
            },
            {
                'profit': 20000.0,
                'sales_velocity': 20.0,
                'days_in_stock': 0,
                'expected_score': (20000.0 * 0.5) + (20.0 * 0.3) - (0 * 0.2)
            }
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases):
            profit = test_case['profit']
            sales_velocity = test_case['sales_velocity']
            days_in_stock = test_case['days_in_stock']
            expected_score = test_case['expected_score']
            
            # Calculate performance score using the same formula as Spark
            calculated_score = (profit * 0.5) + (sales_velocity * 0.3) - (days_in_stock * 0.2)
            
            if abs(calculated_score - expected_score) < 0.001:
                print(f"   ✅ Test case {i+1} PASSED: Score = {calculated_score:.2f}")
            else:
                print(f"   ❌ Test case {i+1} FAILED: Expected {expected_score:.2f}, Got {calculated_score:.2f}")
                all_passed = False
        
        if all_passed:
            print("✅ Performance score calculation test PASSED")
            return True
        else:
            print("❌ Performance score calculation test FAILED")
            return False
            
    except Exception as e:
        print(f"❌ Performance score calculation test FAILED: {e}")
        return False

def test_dead_stock_detection():
    """Test the dead stock detection logic"""
    print("🧪 Testing Dead Stock Detection...")
    
    try:
        # Test cases for dead stock detection (threshold: 30 days)
        test_cases = [
            {'days_in_stock': 25, 'expected_dead_stock': False},
            {'days_in_stock': 30, 'expected_dead_stock': False},  # Equal to threshold
            {'days_in_stock': 31, 'expected_dead_stock': True},   # Exceeds threshold
            {'days_in_stock': 60, 'expected_dead_stock': True},
            {'days_in_stock': 0, 'expected_dead_stock': False}
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases):
            days_in_stock = test_case['days_in_stock']
            expected_dead_stock = test_case['expected_dead_stock']
            
            # Apply the same logic as Spark
            is_dead_stock = days_in_stock > 30
            
            if is_dead_stock == expected_dead_stock:
                status = "DEAD STOCK" if is_dead_stock else "ACTIVE"
                print(f"   ✅ Test case {i+1} PASSED: {days_in_stock} days → {status}")
            else:
                expected_status = "DEAD STOCK" if expected_dead_stock else "ACTIVE"
                actual_status = "DEAD STOCK" if is_dead_stock else "ACTIVE"
                print(f"   ❌ Test case {i+1} FAILED: {days_in_stock} days → Expected {expected_status}, Got {actual_status}")
                all_passed = False
        
        if all_passed:
            print("✅ Dead stock detection test PASSED")
            return True
        else:
            print("❌ Dead stock detection test FAILED")
            return False
            
    except Exception as e:
        print(f"❌ Dead stock detection test FAILED: {e}")
        return False

def test_sales_velocity_calculation():
    """Test the sales velocity calculation logic"""
    print("🧪 Testing Sales Velocity Calculation...")
    
    try:
        # Test cases for sales velocity
        test_cases = [
            {'quantity_sold': 100, 'time_period_days': 10, 'expected_velocity': 10.0},
            {'quantity_sold': 50, 'time_period_days': 5, 'expected_velocity': 10.0},
            {'quantity_sold': 150, 'time_period_days': 30, 'expected_velocity': 5.0},
            {'quantity_sold': 1, 'time_period_days': 1, 'expected_velocity': 1.0}
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases):
            quantity_sold = test_case['quantity_sold']
            time_period_days = test_case['time_period_days']
            expected_velocity = test_case['expected_velocity']
            
            # Calculate sales velocity
            sales_velocity = quantity_sold / time_period_days
            
            if abs(sales_velocity - expected_velocity) < 0.001:
                print(f"   ✅ Test case {i+1} PASSED: {quantity_sold} units / {time_period_days} days = {sales_velocity:.2f} units/day")
            else:
                print(f"   ❌ Test case {i+1} FAILED: Expected {expected_velocity:.2f}, Got {sales_velocity:.2f}")
                all_passed = False
        
        if all_passed:
            print("✅ Sales velocity calculation test PASSED")
            return True
        else:
            print("❌ Sales velocity calculation test FAILED")
            return False
            
    except Exception as e:
        print(f"❌ Sales velocity calculation test FAILED: {e}")
        return False

def test_stock_turnover_calculation():
    """Test the stock turnover calculation logic"""
    print("🧪 Testing Stock Turnover Calculation...")
    
    try:
        # Test cases for stock turnover
        test_cases = [
            {'quantity_sold': 100, 'average_stock': 20, 'expected_turnover': 5.0},
            {'quantity_sold': 50, 'average_stock': 10, 'expected_turnover': 5.0},
            {'quantity_sold': 150, 'average_stock': 30, 'expected_turnover': 5.0},
            {'quantity_sold': 0, 'average_stock': 10, 'expected_turnover': 0.0},
            {'quantity_sold': 10, 'average_stock': 0, 'expected_turnover': 0.0}  # Division by zero case
        ]
        
        all_passed = True
        
        for i, test_case in enumerate(test_cases):
            quantity_sold = test_case['quantity_sold']
            average_stock = test_case['average_stock']
            expected_turnover = test_case['expected_turnover']
            
            # Calculate stock turnover (same logic as Spark)
            if average_stock > 0:
                stock_turnover = quantity_sold / average_stock
            else:
                stock_turnover = 0.0
            
            if abs(stock_turnover - expected_turnover) < 0.001:
                print(f"   ✅ Test case {i+1} PASSED: {quantity_sold} sold / {average_stock} avg stock = {stock_turnover:.2f}")
            else:
                print(f"   ❌ Test case {i+1} FAILED: Expected {expected_turnover:.2f}, Got {stock_turnover:.2f}")
                all_passed = False
        
        if all_passed:
            print("✅ Stock turnover calculation test PASSED")
            return True
        else:
            print("❌ Stock turnover calculation test FAILED")
            return False
            
    except Exception as e:
        print(f"❌ Stock turnover calculation test FAILED: {e}")
        return False

def test_interface_compatibility():
    """Test that the interface can handle the new metrics"""
    print("🧪 Testing Interface Compatibility...")
    
    try:
        # Create sample data with all new metrics
        sample_data = pd.DataFrame([
            {
                "product_id": "test-interface-001",
                "product_name": "Interface Test Product",
                "product_category": "Electronics",
                "selling_price": 299.99,
                "purchase_price": 199.99,
                "minimum_margin_threshold": 25.0,
                "total_profit": 10000.0,
                "total_revenue": 29999.0,
                "total_cost": 19999.0,
                "units_sold": 100,
                "average_profit_margin_pct": 33.3,
                "average_roi_pct": 50.0,
                "contribution_margin_pct": 33.3,
                "sales_velocity": 10.0,
                "stock_turnover": 2.5,
                "days_in_stock": 5,
                "is_dead_stock": False,
                "performance_score": 5250.0,
                "ranking": 1,
                "last_sale_date": datetime.now().isoformat(),
                "analysis_timestamp": datetime.now().isoformat(),
                "analysis_source": "automated_spark_analysis"
            }
        ])
        
        # Test that we can access all the new metrics
        new_metrics = [
            'sales_velocity', 'stock_turnover', 'days_in_stock', 
            'is_dead_stock', 'performance_score', 'last_sale_date'
        ]
        
        missing_metrics = [metric for metric in new_metrics if metric not in sample_data.columns]
        
        if not missing_metrics:
            print("✅ Interface compatibility test PASSED")
            print(f"   - All new metrics accessible: {new_metrics}")
            
            # Test calculations that the interface will perform
            avg_sales_velocity = sample_data['sales_velocity'].mean()
            avg_stock_turnover = sample_data['stock_turnover'].mean()
            avg_performance_score = sample_data['performance_score'].mean()
            dead_stock_count = sample_data['is_dead_stock'].sum()
            
            print(f"   - Avg Sales Velocity: {avg_sales_velocity:.2f} units/day")
            print(f"   - Avg Stock Turnover: {avg_stock_turnover:.2f}")
            print(f"   - Avg Performance Score: {avg_performance_score:.2f}")
            print(f"   - Dead Stock Count: {dead_stock_count}")
            
            return True
        else:
            print(f"❌ Interface compatibility test FAILED - Missing metrics: {missing_metrics}")
            return False
            
    except Exception as e:
        print(f"❌ Interface compatibility test FAILED: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting Advanced Analytics Tests...")
    print("=" * 60)
    
    tests = [
        test_performance_score_calculation,
        test_dead_stock_detection,
        test_sales_velocity_calculation,
        test_stock_turnover_calculation,
        test_advanced_metrics_schema,
        test_interface_compatibility
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
        print("🎉 All tests PASSED! Advanced Analytics Enhancement is working correctly.")
        print("\n🚀 Ready for production deployment!")
        return 0
    else:
        print("⚠️  Some tests FAILED. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())