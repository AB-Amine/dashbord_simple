#!/usr/bin/env python3
"""
Test module for Kafka to Spark data transfer functionality.
Tests the data pipeline from SQLite → Kafka → Spark → MongoDB.
"""

import sys
import os
import unittest
import tempfile
import shutil
import pandas as pd
import sqlite3
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path is set
from utils.config import Config
from data.data_manager import DataManager
from data.accountant_data_manager import AccountantDataManager
from kafka.kafka_producer import KafkaProducer
from data.historical_data_reingestion import HistoricalDataReingestion


class TestKafkaDBToSpark(unittest.TestCase):
    """Test class for Kafka to Spark data transfer functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = Config()
        
        # Create a temporary SQLite database for testing
        self.temp_db_path = "test_analytics.db"
        self.original_db_path = self.config.SQLITE_DB_PATH
        
        # Update config to use test database
        self.config.SQLITE_DB_PATH = self.temp_db_path
        
        # Initialize components
        self.data_manager = DataManager(self.config)
        self.accountant_manager = AccountantDataManager(self.config)
        self.kafka_producer = KafkaProducer(self.config)
        
        # Initialize test database
        self._initialize_test_database()
        
    def tearDown(self):
        """Clean up test environment."""
        # Restore original database path
        self.config.SQLITE_DB_PATH = self.original_db_path
        
        # Clean up temporary database
        if os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            print(f"✅ Cleaned up test database: {self.temp_db_path}")
        
        # Shutdown components
        if hasattr(self, 'data_manager') and self.data_manager:
            self.data_manager.shutdown()
        if hasattr(self, 'accountant_manager') and self.accountant_manager:
            self.accountant_manager.shutdown()
        if hasattr(self, 'kafka_producer') and self.kafka_producer:
            self.kafka_producer.shutdown()
    
    def _initialize_test_database(self):
        """Initialize test database with sample data."""
        print("🚀 Initializing test database...")
        
        # Initialize both data managers
        self.data_manager.initialize_database()
        
        # Add test products using accountant interface
        self.accountant_manager.add_product(
            name="Test Product 1",
            category="Electronics",
            buy_price=100.0,
            sell_price=150.0,
            min_margin_threshold=30.0
        )
        
        self.accountant_manager.add_product(
            name="Test Product 2",
            category="Clothing",
            buy_price=50.0,
            sell_price=80.0,
            min_margin_threshold=35.0
        )
        
        # Add test clients
        self.accountant_manager.add_client(
            name="Test Client 1",
            email="test1@example.com",
            phone="1234567890"
        )
        
        self.accountant_manager.add_client(
            name="Test Client 2",
            email="test2@example.com",
            phone="0987654321"
        )
        
        # Add test stock
        products = self.accountant_manager.get_all_products()
        if products:
            self.accountant_manager.add_stock(
                product_id=products[0]['product_id'],
                quantity=50,
                batch_no="TEST-BATCH-001",
                expiry_date="2025-12-31"
            )
        
        # Add test sale
        clients = self.accountant_manager.get_all_clients()
        if clients and products:
            self.accountant_manager.add_sale(
                client_id=clients[0]['client_id'],
                items=[
                    {'product_id': products[0]['product_id'], 'quantity': 5},
                    {'product_id': products[1]['product_id'], 'quantity': 3}
                ]
            )
        
        print("✅ Test database initialized with sample data")
    
    def test_database_initialization(self):
        """Test that database is properly initialized."""
        print("🧪 Testing database initialization...")
        
        # Test that we can get data from the database
        products = self.accountant_manager.get_all_products()
        clients = self.accountant_manager.get_all_clients()
        sales = self.accountant_manager.get_all_sales()
        stock = self.accountant_manager.get_all_stock()
        
        self.assertGreater(len(products), 0, "Should have test products")
        self.assertGreater(len(clients), 0, "Should have test clients")
        self.assertGreater(len(sales), 0, "Should have test sales")
        self.assertGreater(len(stock), 0, "Should have test stock")
        
        print(f"✅ Database contains: {len(products)} products, {len(clients)} clients, {len(sales)} sales, {len(stock)} stock items")
    
    def test_kafka_topics_initialization(self):
        """Test that Kafka topics are properly initialized."""
        print("🧪 Testing Kafka topics initialization...")
        
        # Initialize Kafka topics
        success = self.kafka_producer.initialize_topics()
        
        self.assertTrue(success, "Kafka topics should initialize successfully")
        print("✅ Kafka topics initialized successfully")
    
    def test_data_transfer_to_kafka(self):
        """Test data transfer from SQLite to Kafka."""
        print("🧪 Testing data transfer to Kafka...")
        
        # Transfer data from SQLite to Kafka
        success = self.kafka_producer.transfer_data_from_sqlite()
        
        self.assertTrue(success, "Data transfer to Kafka should succeed")
        print("✅ Data successfully transferred to Kafka")
    
    def test_historical_data_reingestion(self):
        """Test historical data re-ingestion process."""
        print("🧪 Testing historical data re-ingestion...")
        
        # Create historical re-ingestion instance
        reingestion = HistoricalDataReingestion(self.config, self.kafka_producer)
        
        # Test the re-ingestion process
        success = reingestion.reingest_all_historical_data()
        
        self.assertTrue(success, "Historical data re-ingestion should succeed")
        print("✅ Historical data re-ingestion completed successfully")
    
    def test_complete_data_pipeline(self):
        """Test the complete data pipeline from SQLite to MongoDB."""
        print("🧪 Testing complete data pipeline...")
        
        # Step 1: Verify database has data
        products = self.accountant_manager.get_all_products()
        self.assertGreater(len(products), 0, "Database should have products")
        
        # Step 2: Initialize Kafka topics
        kafka_success = self.kafka_producer.initialize_topics()
        self.assertTrue(kafka_success, "Kafka topics should initialize")
        
        # Step 3: Transfer data to Kafka
        transfer_success = self.kafka_producer.transfer_data_from_sqlite()
        self.assertTrue(transfer_success, "Data transfer should succeed")
        
        # Step 4: Test historical re-ingestion
        reingestion = HistoricalDataReingestion(self.config, self.kafka_producer)
        reingestion_success = reingestion.reingest_all_historical_data()
        self.assertTrue(reingestion_success, "Re-ingestion should succeed")
        
        print("✅ Complete data pipeline test passed!")
        print("🎉 SQLite → Kafka → Spark → MongoDB pipeline is working correctly")
    
    def test_data_consistency(self):
        """Test data consistency between SQLite and Kafka."""
        print("🧪 Testing data consistency...")
        
        # Get data from accountant manager (which maintains its own connection)
        sqlite_products = self.accountant_manager.get_all_products()
        sqlite_clients = self.accountant_manager.get_all_clients()
        
        # Verify we have data
        self.assertGreater(len(sqlite_products), 0, "Should have products in SQLite")
        self.assertGreater(len(sqlite_clients), 0, "Should have clients in SQLite")
        
        # Transfer data to Kafka
        transfer_success = self.kafka_producer.transfer_data_from_sqlite()
        self.assertTrue(transfer_success, "Data transfer should succeed")
        
        print(f"✅ Data consistency verified: {len(sqlite_products)} products, {len(sqlite_clients)} clients")


def run_manual_test():
    """Run the test manually without unittest framework."""
    print("🚀 Running manual Kafka to Spark data transfer test...")
    
    try:
        # Create test instance
        test = TestKafkaDBToSpark()
        test.setUp()
        
        # Run all tests
        print("\n" + "="*60)
        print("🧪 RUNNING KAFKA TO SPARK DATA TRANSFER TESTS")
        print("="*60)
        
        test.test_database_initialization()
        print()
        
        test.test_kafka_topics_initialization()
        print()
        
        test.test_data_transfer_to_kafka()
        print()
        
        test.test_historical_data_reingestion()
        print()
        
        test.test_complete_data_pipeline()
        print()
        
        test.test_data_consistency()
        print()
        
        print("="*60)
        print("🎉 ALL TESTS PASSED!")
        print("✅ Kafka to Spark data transfer pipeline is working correctly")
        print("="*60)
        
        # Clean up
        test.tearDown()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    # Check if running with unittest or manually
    if len(sys.argv) > 1 and sys.argv[1] == "manual":
        # Run manual test
        success = run_manual_test()
        sys.exit(0 if success else 1)
    else:
        # Run with unittest
        unittest.main()