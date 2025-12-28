#!/usr/bin/env python3
"""
Test module for MongoDB collection display functionality.
Tests the admin interface's ability to read and display MongoDB data.
"""

import sys
import os
import unittest
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path is set
from utils.config import Config
from interface.user_interface import UserInterface


class TestMongoDBCollections(unittest.TestCase):
    """Test class for MongoDB collection display functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = Config()
        self.ui = UserInterface(self.config)
        
        # Get MongoDB client
        self.mongo_client = self.ui.get_mongo_client()
        self.db = self.mongo_client[self.config.DB_NAME]
        
        print("✅ Test environment set up successfully")
    
    def tearDown(self):
        """Clean up test environment."""
        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
        
        if hasattr(self, 'ui') and self.ui:
            self.ui.shutdown()
            self.ui = None
        
        print("✅ Test environment cleaned up")
    
    def test_mongodb_connection(self):
        """Test MongoDB connection."""
        print("🧪 Testing MongoDB connection...")
        
        # Test that we can connect to MongoDB
        self.assertIsNotNone(self.mongo_client, "MongoDB client should be created")
        
        # Test that we can access the database
        self.assertIsNotNone(self.db, "Database should be accessible")
        
        # Test that we can list collections
        collections = self.db.list_collection_names()
        self.assertIsNotNone(collections, "Should be able to list collections")
        self.assertIsInstance(collections, list, "Collections should be a list")
        
        print(f"✅ MongoDB connection successful - found {len(collections)} collections")
    
    def test_get_mongo_data(self):
        """Test the get_mongo_data method."""
        print("🧪 Testing get_mongo_data method...")
        
        # Get all collections
        collections = self.db.list_collection_names()
        analytics_collections = [col for col in collections if not col.startswith('system.')]
        
        if not analytics_collections:
            print("⚠️  No analytics collections found - creating test data")
            self._create_test_mongodb_data()
            analytics_collections = [col for col in self.db.list_collection_names() if not col.startswith('system.')]
        
        # Test getting data from each collection
        for collection_name in analytics_collections:
            df = self.ui.get_mongo_data(collection_name)
            
            self.assertIsNotNone(df, f"Should get data from collection {collection_name}")
            self.assertIsInstance(df, pd.DataFrame, f"Data from {collection_name} should be a DataFrame")
            
            print(f"✅ Successfully retrieved {len(df)} records from collection '{collection_name}'")
    
    def test_collection_statistics(self):
        """Test collection statistics calculation."""
        print("🧪 Testing collection statistics...")
        
        # Create test data if needed
        collections = self.db.list_collection_names()
        analytics_collections = [col for col in collections if not col.startswith('system.')]
        
        if not analytics_collections:
            self._create_test_mongodb_data()
            analytics_collections = [col for col in self.db.list_collection_names() if not col.startswith('system.')]
        
        # Test statistics for each collection
        for collection_name in analytics_collections:
            df = self.ui.get_mongo_data(collection_name)
            
            if not df.empty:
                # Test basic statistics
                record_count = len(df)
                column_count = len(df.columns)
                
                self.assertGreater(record_count, 0, f"Collection {collection_name} should have records")
                self.assertGreater(column_count, 0, f"Collection {collection_name} should have columns")
                
                # Test numeric statistics if available
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    stats = df[numeric_cols].describe()
                    self.assertIsNotNone(stats, f"Should get statistics for numeric columns in {collection_name}")
                    print(f"✅ Collection '{collection_name}': {record_count} records, {column_count} columns, {len(numeric_cols)} numeric columns")
                else:
                    print(f"✅ Collection '{collection_name}': {record_count} records, {column_count} columns (no numeric data)")
            else:
                print(f"⚠️  Collection '{collection_name}' is empty")
    
    def test_data_export_formats(self):
        """Test data export in different formats."""
        print("🧪 Testing data export formats...")
        
        # Create test data if needed
        collections = self.db.list_collection_names()
        analytics_collections = [col for col in collections if not col.startswith('system.')]
        
        if not analytics_collections:
            self._create_test_mongodb_data()
            analytics_collections = [col for col in self.db.list_collection_names() if not col.startswith('system.')]
        
        # Test export formats for each collection
        for collection_name in analytics_collections:
            df = self.ui.get_mongo_data(collection_name)
            
            if not df.empty:
                # Test CSV export
                csv_data = df.to_csv(index=False)
                self.assertIsNotNone(csv_data, f"Should export {collection_name} to CSV")
                self.assertIsInstance(csv_data, str, f"CSV data should be a string")
                
                # Test JSON export
                json_data = df.to_json(orient='records', indent=2)
                self.assertIsNotNone(json_data, f"Should export {collection_name} to JSON")
                self.assertIsInstance(json_data, str, f"JSON data should be a string")
                
                print(f"✅ Collection '{collection_name}': CSV and JSON export successful")
                break  # Just test one collection for export formats
    
    def test_collection_visualization_data(self):
        """Test data preparation for visualizations."""
        print("🧪 Testing collection visualization data...")
        
        # Create test data if needed
        collections = self.db.list_collection_names()
        analytics_collections = [col for col in collections if not col.startswith('system.')]
        
        if not analytics_collections:
            self._create_test_mongodb_data()
            analytics_collections = [col for col in self.db.list_collection_names() if not col.startswith('system.')]
        
        # Test visualization data for each collection
        for collection_name in analytics_collections:
            df = self.ui.get_mongo_data(collection_name)
            
            if not df.empty:
                # Test numeric columns for line charts
                numeric_cols = df.select_dtypes(include=['number']).columns
                if len(numeric_cols) > 0:
                    for col in numeric_cols:
                        chart_data = df[col]
                        self.assertIsNotNone(chart_data, f"Should get chart data for column {col}")
                        self.assertGreater(len(chart_data), 0, f"Chart data for {col} should not be empty")
                
                # Test categorical columns for bar charts
                categorical_cols = df.select_dtypes(include=['object']).columns
                if len(categorical_cols) > 0:
                    for col in categorical_cols:
                        if len(df[col].unique()) <= 20:  # Only test if reasonable number of unique values
                            value_counts = df[col].value_counts()
                            self.assertIsNotNone(value_counts, f"Should get value counts for column {col}")
                            self.assertGreater(len(value_counts), 0, f"Value counts for {col} should not be empty")
                
                print(f"✅ Collection '{collection_name}': Visualization data prepared successfully")
                break  # Just test one collection for visualization data
    
    def _create_test_mongodb_data(self):
        """Create test data in MongoDB for testing."""
        print("🚀 Creating test MongoDB data...")
        
        # Create test collections if they don't exist
        test_collections = [
            {
                'name': 'test_products',
                'data': [
                    {'product_id': 'P001', 'name': 'Test Product 1', 'price': 100.0, 'category': 'Electronics'},
                    {'product_id': 'P002', 'name': 'Test Product 2', 'price': 50.0, 'category': 'Clothing'},
                    {'product_id': 'P003', 'name': 'Test Product 3', 'price': 75.0, 'category': 'Electronics'}
                ]
            },
            {
                'name': 'test_clients',
                'data': [
                    {'client_id': 'C001', 'name': 'Test Client 1', 'revenue': 1000.0, 'tier': 'Gold'},
                    {'client_id': 'C002', 'name': 'Test Client 2', 'revenue': 500.0, 'tier': 'Silver'},
                    {'client_id': 'C003', 'name': 'Test Client 3', 'revenue': 2000.0, 'tier': 'Platinum'}
                ]
            }
        ]
        
        for collection_info in test_collections:
            collection = self.db[collection_info['name']]
            
            # Clear existing data
            collection.delete_many({})
            
            # Insert test data
            if collection_info['data']:
                collection.insert_many(collection_info['data'])
                print(f"✅ Created test collection '{collection_info['name']}' with {len(collection_info['data'])} records")


def run_manual_test():
    """Run the test manually without unittest framework."""
    print("🚀 Running manual MongoDB collections test...")
    
    try:
        # Create test instance
        test = TestMongoDBCollections()
        test.setUp()
        
        # Run all tests
        print("\n" + "="*60)
        print("🧪 RUNNING MONGODB COLLECTIONS TESTS")
        print("="*60)
        
        test.test_mongodb_connection()
        print()
        
        test.test_get_mongo_data()
        print()
        
        test.test_collection_statistics()
        print()
        
        test.test_data_export_formats()
        print()
        
        test.test_collection_visualization_data()
        print()
        
        print("="*60)
        print("🎉 ALL MONGODB COLLECTIONS TESTS PASSED!")
        print("✅ MongoDB data display functionality is working correctly")
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