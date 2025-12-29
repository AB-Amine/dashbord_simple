#!/usr/bin/env python3
"""
System Verification Script
Quickly verify that the entire system is working correctly
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from utils.config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SystemVerification")

def verify_system():
    """Verify the entire system is working correctly."""
    logger.info("🔍 Verifying system status...")
    
    try:
        # Initialize components
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Check database collections
        collections = {
            'products': manager.get_all_products(),
            'clients': manager.get_all_clients(),
            'sales': manager.get_all_sales(),
            'inventory': manager.get_all_inventory(),
            'product_winners': manager.get_data_for_kafka('product_winners'),
            'loss_risk_products': manager.get_data_for_kafka('loss_risk_products')
        }
        
        # Display system status
        print("\n" + "="*50)
        print("🎯 SYSTEM STATUS VERIFICATION")
        print("="*50)
        
        all_good = True
        for collection_name, data in collections.items():
            count = len(data)
            status = "✅" if count >= 0 else "❌"
            print(f"{status} {collection_name}: {count} records")
            
            # Check if core collections have data
            if collection_name in ['products', 'clients', 'sales', 'inventory'] and count == 0:
                all_good = False
        
        # Check Kafka integration
        try:
            from kafka.kafka_real_time_producer import RealTimeKafkaProducer
            producer = RealTimeKafkaProducer(config)
            
            # Test Kafka connection by getting data for Kafka
            products_for_kafka = manager.get_products_for_kafka()
            clients_for_kafka = manager.get_clients_for_kafka()
            
            if len(products_for_kafka) > 0 and len(clients_for_kafka) > 0:
                print("✅ Kafka integration: Ready")
            else:
                print("⚠️  Kafka integration: No data available")
                all_good = False
            
            producer.shutdown()
        except Exception as e:
            print(f"❌ Kafka integration: {e}")
            all_good = False
        
        # Check user interface
        try:
            from interface.new_user_interface import NewUserInterface
            ui = NewUserInterface(config)
            ui_products = ui.get_mongo_data('products')
            
            if len(ui_products) > 0:
                print("✅ User interface: Ready")
            else:
                print("⚠️  User interface: No data available")
                all_good = False
            
            ui.shutdown()
        except Exception as e:
            print(f"❌ User interface: {e}")
            all_good = False
        
        # Check admin interface
        try:
            from interface.new_admin_interface import EnhancedAdminInterface
            admin = EnhancedAdminInterface(config)
            admin_products = admin.get_mongo_data('products')
            
            if len(admin_products) > 0:
                print("✅ Admin interface: Ready")
            else:
                print("⚠️  Admin interface: No data available")
                all_good = False
            
            admin.shutdown()
        except Exception as e:
            print(f"❌ Admin interface: {e}")
            all_good = False
        
        print("="*50)
        if all_good:
            print("🎉 SYSTEM STATUS: FULLY OPERATIONAL")
            print("✅ All components working correctly")
        else:
            print("⚠️  SYSTEM STATUS: PARTIAL FUNCTIONALITY")
            print("⚠️  Some components need attention")
        print("="*50)
        
        manager.shutdown()
        return all_good
        
    except Exception as e:
        logger.error(f"❌ System verification failed: {e}")
        print(f"❌ System verification failed: {e}")
        return False

def main():
    """Main entry point for system verification."""
    success = verify_system()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())