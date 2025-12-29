#!/usr/bin/env python3
"""
System Integration Test
Comprehensive test to verify the entire system is working correctly
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from interface.new_user_interface import NewUserInterface
from interface.new_admin_interface import EnhancedAdminInterface
from kafka.kafka_real_time_producer import RealTimeKafkaProducer
from utils.config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SystemIntegrationTest")

class SystemIntegrationTest:
    def __init__(self):
        """Initialize system integration test."""
        self.config = Config()
        self.results = {}
        
    def test_database_connection(self):
        """Test database connection and basic operations."""
        logger.info("🔧 Testing database connection...")
        
        try:
            manager = MongoDBAccountantManager(self.config)
            
            # Test basic operations
            products = manager.get_all_products()
            clients = manager.get_all_clients()
            sales = manager.get_all_sales()
            inventory = manager.get_all_inventory()
            
            self.results['database'] = {
                'products': len(products),
                'clients': len(clients),
                'sales': len(sales),
                'inventory': len(inventory),
                'success': True
            }
            
            logger.info(f"✅ Database test passed: {len(products)} products, {len(clients)} clients, {len(sales)} sales, {len(inventory)} inventory")
            
            manager.shutdown()
            return True
            
        except Exception as e:
            logger.error(f"❌ Database test failed: {e}")
            self.results['database'] = {'success': False, 'error': str(e)}
            return False
    
    def test_user_interface(self):
        """Test user interface functionality."""
        logger.info("👨‍💼 Testing user interface...")
        
        try:
            ui = NewUserInterface(self.config)
            
            # Test data retrieval through UI
            products_df = ui.get_mongo_data('products')
            clients_df = ui.get_mongo_data('clients')
            sales_df = ui.get_mongo_data('sales')
            inventory_df = ui.get_mongo_data('inventory')
            
            self.results['user_interface'] = {
                'products': len(products_df),
                'clients': len(clients_df),
                'sales': len(sales_df),
                'inventory': len(inventory_df),
                'success': True
            }
            
            logger.info(f"✅ User interface test passed: Retrieved all data successfully")
            
            ui.shutdown()
            return True
            
        except Exception as e:
            logger.error(f"❌ User interface test failed: {e}")
            self.results['user_interface'] = {'success': False, 'error': str(e)}
            return False
    
    def test_admin_interface(self):
        """Test admin interface functionality."""
        logger.info("👨‍🔬 Testing admin interface...")
        
        try:
            admin = EnhancedAdminInterface(self.config)
            
            # Test data retrieval through admin interface
            products_df = admin.get_mongo_data('products')
            clients_df = admin.get_mongo_data('clients')
            
            # Test analytics collections (may be empty initially)
            product_winners_df = admin.get_mongo_data('product_winners')
            loss_risk_df = admin.get_mongo_data('loss_risk_products')
            
            self.results['admin_interface'] = {
                'products': len(products_df),
                'clients': len(clients_df),
                'product_winners': len(product_winners_df),
                'loss_risk_products': len(loss_risk_df),
                'success': True
            }
            
            logger.info(f"✅ Admin interface test passed: Retrieved data and analytics collections")
            
            admin.shutdown()
            return True
            
        except Exception as e:
            logger.error(f"❌ Admin interface test failed: {e}")
            self.results['admin_interface'] = {'success': False, 'error': str(e)}
            return False
    
    def test_kafka_integration(self):
        """Test Kafka integration."""
        logger.info("📥 Testing Kafka integration...")
        
        try:
            producer = RealTimeKafkaProducer(self.config)
            
            # Test data import to Kafka
            success = producer.import_data_to_kafka()
            
            self.results['kafka_integration'] = {
                'success': success
            }
            
            if success:
                logger.info("✅ Kafka integration test passed: Data successfully sent to Kafka")
            else:
                logger.error("❌ Kafka integration test failed: Data import failed")
            
            producer.shutdown()
            return success
            
        except Exception as e:
            logger.error(f"❌ Kafka integration test failed: {e}")
            self.results['kafka_integration'] = {'success': False, 'error': str(e)}
            return False
    
    def test_data_consistency(self):
        """Test data consistency across the system."""
        logger.info("🔍 Testing data consistency...")
        
        try:
            # Get data from different sources
            manager = MongoDBAccountantManager(self.config)
            ui = NewUserInterface(self.config)
            
            # Compare data counts
            manager_products = len(manager.get_all_products())
            ui_products = len(ui.get_mongo_data('products'))
            
            manager_clients = len(manager.get_all_clients())
            ui_clients = len(ui.get_mongo_data('clients'))
            
            consistent = (
                manager_products == ui_products and
                manager_clients == ui_clients
            )
            
            self.results['data_consistency'] = {
                'manager_products': manager_products,
                'ui_products': ui_products,
                'manager_clients': manager_clients,
                'ui_clients': ui_clients,
                'consistent': consistent,
                'success': consistent
            }
            
            if consistent:
                logger.info("✅ Data consistency test passed: All data sources consistent")
            else:
                logger.error("❌ Data consistency test failed: Data sources inconsistent")
            
            manager.shutdown()
            ui.shutdown()
            return consistent
            
        except Exception as e:
            logger.error(f"❌ Data consistency test failed: {e}")
            self.results['data_consistency'] = {'success': False, 'error': str(e)}
            return False
    
    def generate_report(self):
        """Generate a comprehensive test report."""
        logger.info("📊 Generating test report...")
        
        print("\n" + "="*60)
        print("🎯 SYSTEM INTEGRATION TEST REPORT")
        print("="*60)
        
        all_passed = True
        
        for test_name, result in self.results.items():
            status = "✅ PASS" if result.get('success', False) else "❌ FAIL"
            print(f"\n{test_name.upper()}: {status}")
            
            if test_name == 'database':
                print(f"  Products: {result.get('products', 0)}")
                print(f"  Clients: {result.get('clients', 0)}")
                print(f"  Sales: {result.get('sales', 0)}")
                print(f"  Inventory: {result.get('inventory', 0)}")
            
            elif test_name == 'user_interface':
                print(f"  Products: {result.get('products', 0)}")
                print(f"  Clients: {result.get('clients', 0)}")
                print(f"  Sales: {result.get('sales', 0)}")
                print(f"  Inventory: {result.get('inventory', 0)}")
            
            elif test_name == 'admin_interface':
                print(f"  Products: {result.get('products', 0)}")
                print(f"  Clients: {result.get('clients', 0)}")
                print(f"  Product Winners: {result.get('product_winners', 0)}")
                print(f"  Loss Risk Products: {result.get('loss_risk_products', 0)}")
            
            elif test_name == 'data_consistency':
                print(f"  Manager Products: {result.get('manager_products', 0)}")
                print(f"  UI Products: {result.get('ui_products', 0)}")
                print(f"  Manager Clients: {result.get('manager_clients', 0)}")
                print(f"  UI Clients: {result.get('ui_clients', 0)}")
                print(f"  Consistent: {result.get('consistent', False)}")
            
            if not result.get('success', False):
                all_passed = False
                if 'error' in result:
                    print(f"  Error: {result['error']}")
        
        print("\n" + "="*60)
        if all_passed:
            print("🎉 ALL TESTS PASSED - SYSTEM IS FULLY FUNCTIONAL")
        else:
            print("⚠️  SOME TESTS FAILED - SYSTEM NEEDS ATTENTION")
        print("="*60)
        
        return all_passed
    
    def run(self):
        """Run all integration tests."""
        logger.info("🚀 Starting system integration tests...")
        
        # Run all tests
        tests = [
            self.test_database_connection,
            self.test_user_interface,
            self.test_admin_interface,
            self.test_kafka_integration,
            self.test_data_consistency
        ]
        
        passed_tests = 0
        total_tests = len(tests)
        
        for test in tests:
            if test():
                passed_tests += 1
        
        # Generate report
        all_passed = self.generate_report()
        
        logger.info(f"📊 Tests completed: {passed_tests}/{total_tests} passed")
        
        return all_passed

def main():
    """Main entry point for system integration test."""
    test = SystemIntegrationTest()
    success = test.run()
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())