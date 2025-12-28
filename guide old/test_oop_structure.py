#!/usr/bin/env python3
"""
Test script to verify the OOP structure works correctly.
"""

import sys
import os

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/src")

def test_imports():
    """Test that all modules can be imported correctly."""
    print("🧪 Testing OOP structure imports...")
    
    try:
        # Test main application
        from main.application import RealTimeAnalyticsApplication
        print("✅ Main application imported successfully")
        
        # Test configuration
        from utils.config import Config
        print("✅ Config imported successfully")
        
        # Test data manager
        from data.data_manager import DataManager
        print("✅ DataManager imported successfully")
        
        # Test Kafka producer
        from kafka.kafka_producer import KafkaProducer
        print("✅ KafkaProducer imported successfully")
        
        # Test Spark analytics
        from analytics.spark_analytics import SparkAnalytics
        print("✅ SparkAnalytics imported successfully")
        
        # Test user interface
        from interface.user_interface import UserInterface
        print("✅ UserInterface imported successfully")
        
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

def test_config():
    """Test configuration class."""
    print("\n🧪 Testing Config class...")
    
    try:
        from utils.config import Config
        
        config = Config()
        print(f"✅ Config created: {config.DB_NAME}")
        print(f"✅ Kafka topics: {config.KAFKA_TOPICS}")
        print(f"✅ MongoDB URI: {config.MONGO_URI}")
        
        return True
    except Exception as e:
        print(f"❌ Config test failed: {e}")
        return False

def test_data_manager():
    """Test data manager class."""
    print("\n🧪 Testing DataManager class...")
    
    try:
        from utils.config import Config
        from data.data_manager import DataManager
        
        config = Config()
        data_manager = DataManager(config)
        
        # Test database initialization
        result = data_manager.initialize_database()
        print(f"✅ Database initialization: {result}")
        
        # Test getting products (should be empty initially)
        products = data_manager.get_products()
        print(f"✅ Products retrieved: {len(products)} products")
        
        data_manager.shutdown()
        return True
    except Exception as e:
        print(f"❌ DataManager test failed: {e}")
        return False

def test_application_creation():
    """Test application creation."""
    print("\n🧪 Testing Application creation...")
    
    try:
        from main.application import RealTimeAnalyticsApplication
        
        app = RealTimeAnalyticsApplication()
        print(f"✅ Application created successfully")
        print(f"✅ Config: {app.config.DB_NAME}")
        print(f"✅ DataManager: {type(app.data_manager).__name__}")
        print(f"✅ KafkaProducer: {type(app.kafka_producer).__name__}")
        print(f"✅ SparkAnalytics: {type(app.spark_analytics).__name__}")
        print(f"✅ UserInterface: {type(app.user_interface).__name__}")
        
        return True
    except Exception as e:
        print(f"❌ Application creation test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting OOP structure tests...\n")
    
    tests = [
        test_imports,
        test_config,
        test_data_manager,
        test_application_creation
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print(f"\n📊 Test Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("🎉 All tests passed! OOP structure is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())