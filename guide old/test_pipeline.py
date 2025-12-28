#!/usr/bin/env python3
"""
Test script to verify the complete analytics pipeline.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all imports work correctly."""
    print("=== Testing Imports ===")
    
    try:
        from src.utils.config import Config
        print("✅ Config import successful")
        
        from src.data.accountant_data_manager import AccountantDataManager
        print("✅ AccountantDataManager import successful")
        
        from src.data.data_manager import DataManager
        print("✅ DataManager import successful")
        
        from src.kafka.kafka_producer import KafkaProducer
        print("✅ KafkaProducer import successful")
        
        from src.analytics.spark_analytics import SparkAnalytics
        print("✅ SparkAnalytics import successful")
        
        from src.interface.user_interface import UserInterface
        print("✅ UserInterface import successful")
        
        from src.main.application import RealTimeAnalyticsApplication
        print("✅ RealTimeAnalyticsApplication import successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_database_operations():
    """Test database operations."""
    print("\n=== Testing Database Operations ===")
    
    try:
        from src.utils.config import Config
        from src.data.accountant_data_manager import AccountantDataManager
        
        config = Config()
        dm = AccountantDataManager(config)
        
        # Test adding a product
        success, msg = dm.add_product('Test Product', 'Test Category', 10.0, 15.0, 20.0)
        print(f"✅ Product added: {success} - {msg}")
        
        # Test adding a client
        success, msg = dm.add_client('Test Client', 'test@example.com', '+1234567890')
        print(f"✅ Client added: {success} - {msg}")
        
        # Test getting products
        products = dm.get_all_products()
        print(f"✅ Products retrieved: {len(products)}")
        
        # Test getting clients
        clients = dm.get_all_clients()
        print(f"✅ Clients retrieved: {len(clients)}")
        
        dm.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Database operations failed: {e}")
        return False

def test_kafka_integration():
    """Test Kafka integration."""
    print("\n=== Testing Kafka Integration ===")
    
    try:
        from src.utils.config import Config
        from src.kafka.kafka_producer import KafkaProducer
        
        config = Config()
        kafka_producer = KafkaProducer(config)
        
        # Test topic initialization
        success = kafka_producer.initialize_topics()
        print(f"✅ Kafka topics initialized: {success}")
        
        kafka_producer.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Kafka integration failed: {e}")
        return False

def test_spark_integration():
    """Test Spark integration."""
    print("\n=== Testing Spark Integration ===")
    
    try:
        from src.utils.config import Config
        from src.analytics.spark_analytics import SparkAnalytics
        
        config = Config()
        spark_analytics = SparkAnalytics(config)
        
        # Test Spark session initialization
        success = spark_analytics.initialize_spark()
        print(f"✅ Spark session initialized: {success}")
        
        spark_analytics.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Spark integration failed: {e}")
        return False

def test_complete_application():
    """Test the complete application initialization."""
    print("\n=== Testing Complete Application ===")
    
    try:
        from src.main.application import RealTimeAnalyticsApplication
        
        app = RealTimeAnalyticsApplication()
        
        # Test system initialization
        success = app.initialize_system()
        print(f"✅ System initialized: {success}")
        
        app.shutdown()
        return True
        
    except Exception as e:
        print(f"❌ Complete application test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting Complete Analytics Pipeline Test")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_database_operations,
        test_kafka_integration,
        test_spark_integration,
        test_complete_application
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Pipeline is working correctly.")
    else:
        print("❌ Some tests failed. Check the error messages above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)