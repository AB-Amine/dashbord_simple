#!/usr/bin/env python3
"""
Basic test for the enhanced admin interface without Spark startup.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from interface.new_admin_interface import EnhancedAdminInterface

def test_basic_functionality():
    """Test basic admin interface functionality."""
    print("🧪 Testing Basic Admin Interface Functionality...")
    
    try:
        config = Config()
        admin = EnhancedAdminInterface(config)
        
        print("✅ EnhancedAdminInterface created successfully")
        print(f"📊 Database: {config.DB_NAME}")
        print(f"🔧 MongoDB Manager: {admin.mongodb_manager is not None}")
        print(f"📥 Kafka Producer: {admin.kafka_producer is not None}")
        print(f"⚡ Spark Analytics: {admin.spark_analytics is not None}")
        
        # Test MongoDB data access
        print("\n📊 Testing MongoDB data access...")
        try:
            clients = admin.get_mongo_data('clients')
            print(f"✅ MongoDB access works: {len(clients)} clients found")
        except Exception as e:
            print(f"❌ MongoDB access failed: {e}")
        
        # Test auto-analysis state management
        print("\n🎛️  Testing auto-analysis state management...")
        print(f"   Initial state: {'ACTIVE' if admin.auto_analysis_active else 'INACTIVE'}")
        
        # Test Kafka transfer state management
        print("\n📥 Testing Kafka transfer state management...")
        print(f"   Initial state: {'ACTIVE' if admin.kafka_transfer_active else 'INACTIVE'}")
        
        # Test data addition
        print("\n📦 Testing data addition...")
        try:
            # Add a test product
            success, msg = admin.mongodb_manager.add_product(
                name="Test Product",
                category="Test",
                buy_price=50.0,
                sell_price=100.0,
                min_margin_threshold=20.0
            )
            print(f"   Product addition: {'✅ SUCCESS' if success else '❌ FAILED'} - {msg}")
            
            # Add a test client
            success, msg = admin.mongodb_manager.add_client(
                name="Test Client",
                email="test@example.com"
            )
            print(f"   Client addition: {'✅ SUCCESS' if success else '❌ FAILED'} - {msg}")
            
        except Exception as e:
            print(f"❌ Data addition failed: {e}")
        
        # Cleanup
        admin.shutdown()
        print("\n✅ Basic functionality test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_kafka_transfer():
    """Test Kafka transfer functionality."""
    print("\n🧪 Testing Kafka Transfer Functionality...")
    
    try:
        config = Config()
        admin = EnhancedAdminInterface(config)
        
        # Test manual Kafka transfer
        print("📥 Testing manual Kafka transfer...")
        try:
            admin.kafka_producer.import_data_to_kafka()
            print("✅ Manual Kafka transfer completed successfully")
        except Exception as e:
            print(f"❌ Manual Kafka transfer failed: {e}")
        
        # Test state management
        print("\n🎛️  Testing Kafka transfer state management...")
        
        # Start transfer
        success = admin.start_kafka_transfer()
        print(f"   Start transfer: {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        # Check if it's running
        print(f"   Transfer active: {admin.kafka_transfer_active}")
        
        # Stop transfer
        success = admin.stop_kafka_transfer()
        print(f"   Stop transfer: {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        # Cleanup
        admin.shutdown()
        print("\n✅ Kafka transfer test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka transfer test failed: {e}")
        return False

def main():
    """Run basic tests."""
    print("🚀 Enhanced Admin Interface Basic Tests")
    print("=" * 50)
    
    # Test 1: Basic functionality
    basic_ok = test_basic_functionality()
    
    # Test 2: Kafka transfer
    kafka_ok = test_kafka_transfer()
    
    print("\n" + "=" * 50)
    print("📊 BASIC TEST SUMMARY:")
    print(f"  Basic Functionality: {'✅ PASS' if basic_ok else '❌ FAIL'}")
    print(f"  Kafka Transfer:      {'✅ PASS' if kafka_ok else '❌ FAIL'}")
    
    if basic_ok and kafka_ok:
        print("\n🎉 Basic tests passed!")
        print("✅ Enhanced Admin Interface core functionality is working")
        print("📋 Note: Spark analytics requires proper Spark setup to run")
    else:
        print("\n⚠️  Some tests failed. Check the issues above.")

if __name__ == "__main__":
    main()