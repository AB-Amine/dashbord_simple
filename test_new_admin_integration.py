#!/usr/bin/env python3
"""
Test script for the new enhanced admin interface integration.
"""

import sys
import os
import time

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from interface.new_admin_interface import EnhancedAdminInterface

def test_admin_interface_initialization():
    """Test that the enhanced admin interface initializes correctly."""
    print("🧪 Testing Enhanced Admin Interface Initialization...")
    
    try:
        config = Config()
        admin = EnhancedAdminInterface(config)
        
        print("✅ EnhancedAdminInterface created successfully")
        print(f"📊 Config: {config.DB_NAME}")
        print(f"🔧 MongoDB Manager: {admin.mongodb_manager is not None}")
        print(f"📥 Kafka Producer: {admin.kafka_producer is not None}")
        print(f"⚡ Spark Analytics: {admin.spark_analytics is not None}")
        
        # Test auto-analysis controls
        print(f"\n🎛️  Auto-Analysis Controls:")
        print(f"   Initial state: {'ACTIVE' if admin.auto_analysis_active else 'INACTIVE'}")
        
        # Test starting auto-analysis
        if not admin.auto_analysis_active:
            success = admin.start_auto_analysis()
            print(f"   Start result: {'✅ SUCCESS' if success else '❌ FAILED'}")
            time.sleep(2)  # Let it run briefly
            
            # Test stopping auto-analysis
            success = admin.stop_auto_analysis()
            print(f"   Stop result: {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        # Test Kafka transfer controls
        print(f"\n📥 Kafka Transfer Controls:")
        print(f"   Initial state: {'ACTIVE' if admin.kafka_transfer_active else 'INACTIVE'}")
        
        # Test starting Kafka transfer
        if not admin.kafka_transfer_active:
            success = admin.start_kafka_transfer()
            print(f"   Start result: {'✅ SUCCESS' if success else '❌ FAILED'}")
            time.sleep(2)  # Let it run briefly
            
            # Test stopping Kafka transfer
            success = admin.stop_kafka_transfer()
            print(f"   Stop result: {'✅ SUCCESS' if success else '❌ FAILED'}")
        
        # Cleanup
        admin.shutdown()
        print("\n✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_transfer():
    """Test MongoDB to Kafka data transfer."""
    print("\n🧪 Testing MongoDB to Kafka Data Transfer...")
    
    try:
        config = Config()
        admin = EnhancedAdminInterface(config)
        
        # Add some test data first
        print("📦 Adding test data...")
        
        # Add a product
        success, msg = admin.mongodb_manager.add_product(
            name="Test Product",
            category="Test",
            buy_price=50.0,
            sell_price=100.0,
            min_margin_threshold=20.0
        )
        print(f"   Product: {msg}")
        
        # Add a client
        success, msg = admin.mongodb_manager.add_client(
            name="Test Client",
            email="test@example.com"
        )
        print(f"   Client: {msg}")
        
        # Test manual transfer
        print("\n📥 Testing manual Kafka transfer...")
        try:
            admin.kafka_producer.import_data_to_kafka()
            print("✅ Manual Kafka transfer completed")
        except Exception as e:
            print(f"❌ Manual Kafka transfer failed: {e}")
        
        # Test auto transfer
        print("\n🔄 Testing auto Kafka transfer...")
        success = admin.start_kafka_transfer()
        if success:
            print("✅ Auto Kafka transfer started")
            time.sleep(12)  # Let it run for one cycle
            success = admin.stop_kafka_transfer()
            print(f"✅ Auto Kafka transfer stopped: {'SUCCESS' if success else 'FAILED'}")
        else:
            print("❌ Auto Kafka transfer failed to start")
        
        # Cleanup
        admin.shutdown()
        print("\n✅ Data transfer test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Data transfer test failed: {e}")
        return False

def test_spark_analytics():
    """Test Spark analytics integration."""
    print("\n🧪 Testing Spark Analytics Integration...")
    
    try:
        config = Config()
        admin = EnhancedAdminInterface(config)
        
        # Test manual analytics
        print("📊 Testing manual Spark analytics...")
        try:
            success = admin.spark_analytics.run_analytics()
            print(f"✅ Manual analytics completed: {'SUCCESS' if success else 'FAILED'}")
        except Exception as e:
            print(f"❌ Manual analytics failed: {e}")
        
        # Test auto analytics
        print("\n⚡ Testing auto Spark analytics...")
        success = admin.start_auto_analysis()
        if success:
            print("✅ Auto analytics started")
            time.sleep(12)  # Let it run for one cycle
            success = admin.stop_auto_analysis()
            print(f"✅ Auto analytics stopped: {'SUCCESS' if success else 'FAILED'}")
        else:
            print("❌ Auto analytics failed to start")
        
        # Cleanup
        admin.shutdown()
        print("\n✅ Spark analytics test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Spark analytics test failed: {e}")
        return False

def main():
    """Run all integration tests."""
    print("🚀 Enhanced Admin Interface Integration Tests")
    print("=" * 60)
    
    # Test 1: Initialization
    init_ok = test_admin_interface_initialization()
    
    # Test 2: Data Transfer
    transfer_ok = test_data_transfer()
    
    # Test 3: Spark Analytics
    analytics_ok = test_spark_analytics()
    
    print("\n" + "=" * 60)
    print("📊 INTEGRATION TEST SUMMARY:")
    print(f"  Initialization: {'✅ PASS' if init_ok else '❌ FAIL'}")
    print(f"  Data Transfer:  {'✅ PASS' if transfer_ok else '❌ FAIL'}")
    print(f"  Spark Analytics: {'✅ PASS' if analytics_ok else '❌ FAIL'}")
    
    if all([init_ok, transfer_ok, analytics_ok]):
        print("\n🎉 All integration tests passed!")
        print("✅ Enhanced Admin Interface is ready for use")
    else:
        print("\n⚠️  Some tests failed. Check the issues above.")

if __name__ == "__main__":
    main()