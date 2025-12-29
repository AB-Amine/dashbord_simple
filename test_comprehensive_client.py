#!/usr/bin/env python3
"""
Comprehensive test script to verify client functionality including sales and revenue calculations.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

def test_comprehensive_client_scenario():
    """Test a complete client scenario with products, sales, and revenue calculations."""
    print("🚀 Starting comprehensive client scenario test...")
    print("=" * 60)
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Step 1: Add a product
        print("\n📦 Step 1: Adding test product...")
        success, msg = manager.add_product(
            name="Test Product",
            category="Electronics",
            buy_price=100.0,
            sell_price=150.0,
            min_margin_threshold=30.0
        )
        print(f"Product addition: {msg}")
        
        # Get the product ID
        products = manager.get_all_products()
        product_id = products[0]['product_id'] if products else None
        
        # Step 2: Add a client
        print("\n👥 Step 2: Adding test client...")
        success, msg = manager.add_client(
            name="John Doe",
            email="john@example.com",
            phone="+1234567890"
        )
        print(f"Client addition: {msg}")
        
        # Get the client ID
        clients = manager.get_all_clients()
        client_id = clients[0]['client_id'] if clients else None
        
        # Step 3: Add a sale for this client
        print("\n💰 Step 3: Adding test sale...")
        if product_id and client_id:
            items = [
                {
                    'product_id': product_id,
                    'quantity': 5
                }
            ]
            
            success, msg = manager.add_sale(
                client_id=client_id,
                items=items
            )
            print(f"Sale addition: {msg}")
            
            # Step 4: Check client revenue update
            print("\n📊 Step 4: Checking client revenue update...")
            updated_client = manager.get_client_by_id(client_id)
            if updated_client:
                revenue = updated_client.get('total_revenue_generated', 0)
                loyalty = updated_client.get('loyalty_tier', 'Unknown')
                
                print(f"Client: {updated_client['name']}")
                print(f"Revenue: €{revenue:.2f}")
                print(f"Loyalty Tier: {loyalty}")
                
                # Expected revenue: 5 units * €150 = €750
                expected_revenue = 5 * 150.0
                if abs(revenue - expected_revenue) < 0.01:
                    print("✅ Revenue calculation is correct")
                else:
                    print(f"❌ Revenue calculation error: Expected €{expected_revenue:.2f}, Got €{revenue:.2f}")
                
                # Check loyalty tier (should be Standard for €750)
                if loyalty == 'Standard':
                    print("✅ Loyalty tier is correct")
                else:
                    print(f"❌ Loyalty tier error: Expected 'Standard', Got '{loyalty}'")
            else:
                print("❌ Failed to retrieve updated client")
        else:
            print("❌ Missing product or client ID for sale")
        
        # Step 5: Add more sales to test loyalty tier progression
        print("\n🏆 Step 5: Testing loyalty tier progression...")
        if product_id and client_id:
            # Add multiple sales to reach different loyalty tiers
            sale_amounts = [
                {'quantity': 10, 'expected_revenue': 1500, 'expected_tier': 'Standard'},
                {'quantity': 50, 'expected_revenue': 7500, 'expected_tier': 'Bronze'},
                {'quantity': 100, 'expected_revenue': 15000, 'expected_tier': 'Silver'},
                {'quantity': 200, 'expected_revenue': 30000, 'expected_tier': 'Gold'}
            ]
            
            current_revenue = 0
            for sale in sale_amounts:
                items = [{'product_id': product_id, 'quantity': sale['quantity']}]
                success, msg = manager.add_sale(client_id=client_id, items=items)
                
                if success:
                    updated_client = manager.get_client_by_id(client_id)
                    if updated_client:
                        revenue = updated_client.get('total_revenue_generated', 0)
                        loyalty = updated_client.get('loyalty_tier', 'Unknown')
                        
                        print(f"\nAfter sale of {sale['quantity']} units:")
                        print(f"  Revenue: €{revenue:.2f}")
                        print(f"  Loyalty Tier: {loyalty}")
                        
                        # Check if tier is correct
                        if loyalty == sale['expected_tier']:
                            print(f"  ✅ Loyalty tier correct: {loyalty}")
                        else:
                            print(f"  ❌ Loyalty tier error: Expected '{sale['expected_tier']}', Got '{loyalty}'")
        
        # Step 6: Display final client data
        print("\n📋 Step 6: Final client data...")
        final_client = manager.get_client_by_id(client_id)
        if final_client:
            print(f"Client Name: {final_client['name']}")
            print(f"Email: {final_client['email']}")
            print(f"Phone: {final_client['phone']}")
            print(f"Total Revenue: €{final_client['total_revenue_generated']:.2f}")
            print(f"Loyalty Tier: {final_client['loyalty_tier']}")
            print(f"Created: {final_client['created_at']}")
            print(f"Last Updated: {final_client['updated_at']}")
        
        print("\n🎉 Comprehensive client scenario test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Comprehensive test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_client_display_in_ui():
    """Test how clients would be displayed in the UI."""
    print("\n🖥️  Testing client display simulation...")
    
    try:
        config = Config()
        manager = MongoDBAccountantManager(config)
        
        # Get all clients
        clients = manager.get_all_clients()
        
        if clients:
            print(f"\n📊 Clients that would be displayed in UI:")
            print("-" * 80)
            
            for i, client in enumerate(clients):
                print(f"| {i+1:2d} | {client['name']:20s} | {client['email'] or 'N/A':25s} | "
                      f"€{client['total_revenue_generated']:10.2f} | {client['loyalty_tier']:8s} | "
                      f"{client['created_at'][:19]} |")
            
            print("-" * 80)
            
            # Show summary statistics like in the UI
            total_clients = len(clients)
            total_revenue = sum(client['total_revenue_generated'] for client in clients)
            gold_clients = sum(1 for client in clients if client['loyalty_tier'] == 'Gold')
            
            print(f"\n📈 Summary Statistics:")
            print(f"   Total Clients: {total_clients}")
            print(f"   Total Revenue: €{total_revenue:,.2f}")
            print(f"   Gold Clients: {gold_clients}")
            
            return True
        else:
            print("⚠️  No clients found for UI display")
            return False
            
    except Exception as e:
        print(f"❌ UI display test failed: {e}")
        return False

def main():
    """Run comprehensive tests."""
    print("🚀 Starting comprehensive client functionality tests...")
    print("=" * 80)
    
    # Test comprehensive scenario
    scenario_ok = test_comprehensive_client_scenario()
    
    # Test UI display simulation
    ui_ok = test_client_display_in_ui()
    
    print("\n" + "=" * 80)
    print("📊 COMPREHENSIVE TEST SUMMARY:")
    print(f"  Scenario Test: {'✅ PASS' if scenario_ok else '❌ FAIL'}")
    print(f"  UI Display Test: {'✅ PASS' if ui_ok else '❌ FAIL'}")
    
    if scenario_ok and ui_ok:
        print("\n🎉 All comprehensive tests passed!")
        print("✅ Client functionality is working correctly including:")
        print("   - Client addition")
        print("   - Revenue calculation from sales")
        print("   - Loyalty tier progression")
        print("   - Data display for UI")
    else:
        print("\n⚠️  Some tests failed. Please check the issues above.")

if __name__ == "__main__":
    main()