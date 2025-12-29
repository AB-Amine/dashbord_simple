#!/usr/bin/env python3
"""
Test script to verify MongoDB client addition and display functionality.
This script will test the complete flow of adding clients and retrieving them.
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager
from pymongo import MongoClient

def test_mongodb_connection():
    """Test MongoDB connection and basic operations."""
    print("🔍 Testing MongoDB connection...")
    
    try:
        # Test direct MongoDB connection
        config = Config()
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        # Test if we can list collections
        collections = db.list_collection_names()
        print(f"✅ MongoDB connection successful")
        print(f"📊 Available collections: {collections}")
        
        # Test if clients collection exists
        if 'clients' in collections:
            print("✅ Clients collection exists")
            clients_count = db.clients.count_documents({})
            print(f"📊 Current clients count: {clients_count}")
        else:
            print("⚠️  Clients collection does not exist yet")
            
        return True
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False

def test_client_addition():
    """Test adding a client through MongoDBAccountantManager."""
    print("\n🔍 Testing client addition...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Add a test client
        success, message = manager.add_client(
            name="Test Client - Debug",
            email="test-debug@example.com",
            phone="+1234567890"
        )
        
        print(f"Add client result: Success={success}, Message='{message}'")
        
        if success:
            # Verify the client was added
            clients = manager.get_all_clients()
            test_client = None
            
            for client in clients:
                if client.get('name') == "Test Client - Debug":
                    test_client = client
                    break
            
            if test_client:
                print("✅ Client successfully added and retrieved")
                print(f"📊 Client details: {test_client}")
            else:
                print("❌ Client was not found after addition")
                
            # Show all clients
            print(f"📊 Total clients in database: {len(clients)}")
            for i, client in enumerate(clients):
                print(f"  {i+1}. {client.get('name')} (ID: {client.get('client_id')})")
        else:
            print(f"❌ Client addition failed: {message}")
            
        return success
        
    except Exception as e:
        print(f"❌ Client addition test failed: {e}")
        return False

def test_client_retrieval():
    """Test retrieving clients directly from MongoDB."""
    print("\n🔍 Testing direct client retrieval...")
    
    try:
        config = Config()
        client = MongoClient(config.MONGO_URI)
        db = client[config.DB_NAME]
        
        # Get all clients directly from MongoDB
        clients = list(db.clients.find({}, {'_id': 0}))
        
        print(f"✅ Retrieved {len(clients)} clients directly from MongoDB")
        
        for i, client_data in enumerate(clients):
            print(f"  {i+1}. {client_data.get('name')} (ID: {client_data.get('client_id')})")
            print(f"     Email: {client_data.get('email')}")
            print(f"     Revenue: €{client_data.get('total_revenue_generated', 0):.2f}")
            print(f"     Loyalty: {client_data.get('loyalty_tier')}")
            print("-" * 50)
        
        return True
        
    except Exception as e:
        print(f"❌ Direct client retrieval failed: {e}")
        return False

def test_client_parameters():
    """Test client parameter calculations."""
    print("\n🔍 Testing client parameter calculations...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Get all clients
        clients = manager.get_all_clients()
        
        if not clients:
            print("⚠️  No clients found to test parameters")
            return False
        
        print(f"📊 Analyzing {len(clients)} clients...")
        
        for client in clients:
            name = client.get('name', 'Unknown')
            revenue = client.get('total_revenue_generated', 0)
            loyalty = client.get('loyalty_tier', 'Unknown')
            
            print(f"\n👤 Client: {name}")
            print(f"   Revenue: €{revenue:.2f}")
            print(f"   Loyalty Tier: {loyalty}")
            
            # Check if loyalty tier is calculated correctly
            if revenue > config.REVENUE_THRESHOLD_GOLD:
                expected_tier = 'Gold'
            elif revenue > config.REVENUE_THRESHOLD_SILVER:
                expected_tier = 'Silver'
            elif revenue > config.REVENUE_THRESHOLD_PROMO:
                expected_tier = 'Bronze'
            else:
                expected_tier = 'Standard'
            
            if loyalty == expected_tier:
                print(f"   ✅ Loyalty tier is correct")
            else:
                print(f"   ❌ Loyalty tier mismatch: Expected '{expected_tier}', Got '{loyalty}'")
        
        return True
        
    except Exception as e:
        print(f"❌ Client parameter test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting MongoDB client functionality tests...")
    print("=" * 60)
    
    # Test 1: MongoDB connection
    connection_ok = test_mongodb_connection()
    
    # Test 2: Client addition
    addition_ok = test_client_addition()
    
    # Test 3: Direct client retrieval
    retrieval_ok = test_client_retrieval()
    
    # Test 4: Client parameter calculations
    params_ok = test_client_parameters()
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY:")
    print(f"  Connection: {'✅ PASS' if connection_ok else '❌ FAIL'}")
    print(f"  Addition:   {'✅ PASS' if addition_ok else '❌ FAIL'}")
    print(f"  Retrieval:  {'✅ PASS' if retrieval_ok else '❌ FAIL'}")
    print(f"  Parameters: {'✅ PASS' if params_ok else '❌ FAIL'}")
    
    if all([connection_ok, addition_ok, retrieval_ok, params_ok]):
        print("\n🎉 All tests passed! Client functionality is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Please check the issues above.")

if __name__ == "__main__":
    main()