#!/usr/bin/env python3
"""
Clean test to verify loyalty tier progression with fresh data.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager
from pymongo import MongoClient

def clean_database():
    """Clean the database before testing."""
    print("🧹 Cleaning database...")
    
    config = Config()
    client = MongoClient(config.MONGO_URI)
    db = client[config.DB_NAME]
    
    # Drop all collections
    collections = db.list_collection_names()
    for collection in collections:
        db[collection].drop()
    
    print("✅ Database cleaned")

def test_loyalty_progression():
    """Test loyalty tier progression with fresh data."""
    print("\n🚀 Testing loyalty tier progression with fresh data...")
    
    config = Config()
    manager = MongoDBAccountantManager(config)
    
    # Step 1: Add a product
    print("\n📦 Adding test product...")
    success, msg = manager.add_product(
        name="Loyalty Test Product",
        category="Test",
        buy_price=50.0,
        sell_price=100.0,
        min_margin_threshold=20.0
    )
    print(f"Product: {msg}")
    
    # Get product ID
    products = manager.get_all_products()
    product_id = products[0]['product_id']
    
    # Step 2: Add a client
    print("\n👥 Adding test client...")
    success, msg = manager.add_client(
        name="Loyalty Test Client",
        email="loyalty-test@example.com"
    )
    print(f"Client: {msg}")
    
    # Get client ID
    clients = manager.get_all_clients()
    client_id = clients[0]['client_id']
    
    # Step 3: Test loyalty progression
    print("\n🏆 Testing loyalty tier progression:")
    
    test_cases = [
        {'quantity': 5, 'expected_revenue': 500, 'expected_tier': 'Standard', 'description': 'Initial sale'},
        {'quantity': 6, 'expected_revenue': 1100, 'expected_tier': 'Bronze', 'description': 'Reach Bronze tier'},
        {'quantity': 10, 'expected_revenue': 2100, 'expected_tier': 'Bronze', 'description': 'Stay in Bronze'},
        {'quantity': 80, 'expected_revenue': 10100, 'expected_tier': 'Silver', 'description': 'Reach Silver tier'},
        {'quantity': 100, 'expected_revenue': 20100, 'expected_tier': 'Gold', 'description': 'Reach Gold tier'},
    ]
    
    for i, test_case in enumerate(test_cases):
        print(f"\n--- Step {i+1}: {test_case['description']} ---")
        
        # Add sale
        items = [{'product_id': product_id, 'quantity': test_case['quantity']}]
        success, msg = manager.add_sale(client_id=client_id, items=items)
        print(f"Sale: {msg}")
        
        # Check client status
        client = manager.get_client_by_id(client_id)
        if client:
            revenue = client['total_revenue_generated']
            loyalty = client['loyalty_tier']
            
            print(f"Revenue: €{revenue:.2f}")
            print(f"Tier: {loyalty}")
            
            # Check if values match expectations
            revenue_ok = abs(revenue - test_case['expected_revenue']) < 0.01
            tier_ok = loyalty == test_case['expected_tier']
            
            if revenue_ok and tier_ok:
                print("✅ PASS: Revenue and tier are correct")
            else:
                print(f"❌ FAIL: ", end="")
                if not revenue_ok:
                    print(f"Revenue mismatch (expected €{test_case['expected_revenue']:.2f}) ", end="")
                if not tier_ok:
                    print(f"Tier mismatch (expected {test_case['expected_tier']})")
        else:
            print("❌ Failed to retrieve client")
    
    # Final summary
    print(f"\n📋 Final Client Summary:")
    final_client = manager.get_client_by_id(client_id)
    if final_client:
        print(f"Name: {final_client['name']}")
        print(f"Email: {final_client['email']}")
        print(f"Total Revenue: €{final_client['total_revenue_generated']:.2f}")
        print(f"Loyalty Tier: {final_client['loyalty_tier']}")
        
        # Verify final tier
        revenue = final_client['total_revenue_generated']
        if revenue > config.REVENUE_THRESHOLD_GOLD:
            expected = 'Gold'
        elif revenue > config.REVENUE_THRESHOLD_SILVER:
            expected = 'Silver'
        elif revenue > config.REVENUE_THRESHOLD_PROMO:
            expected = 'Bronze'
        else:
            expected = 'Standard'
        
        if final_client['loyalty_tier'] == expected:
            print("✅ Final loyalty tier is correct")
        else:
            print(f"❌ Final loyalty tier error: Expected {expected}, Got {final_client['loyalty_tier']}")

def main():
    """Run the clean loyalty test."""
    print("🎯 Clean Loyalty Tier Progression Test")
    print("=" * 50)
    
    # Clean database first
    clean_database()
    
    # Run the test
    test_loyalty_progression()
    
    print("\n🎉 Test completed!")

if __name__ == "__main__":
    main()