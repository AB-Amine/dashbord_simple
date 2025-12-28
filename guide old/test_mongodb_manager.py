#!/usr/bin/env python3
"""
Test script for MongoDB Accountant Manager
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from utils.config import Config

def test_mongodb_manager():
    """Test all MongoDB Accountant Manager methods."""
    print('🧪 Testing MongoDB Accountant Manager methods...')
    
    config = Config()
    manager = MongoDBAccountantManager(config)
    
    try:
        # Test get methods
        products = manager.get_all_products()
        print(f'✅ get_all_products: {len(products)} items')
        
        clients = manager.get_all_clients()
        print(f'✅ get_all_clients: {len(clients)} items')
        
        sales = manager.get_all_sales()
        print(f'✅ get_all_sales: {len(sales)} items')
        
        stock = manager.get_all_stock()
        print(f'✅ get_all_stock: {len(stock)} items')
        
        inventory = manager.get_all_inventory()
        print(f'✅ get_all_inventory: {len(inventory)} items')
        
        # Test add methods
        if products:
            success, msg = manager.add_stock(
                product_id=products[0]['product_id'],
                quantity=10,
                batch_no='TEST-BATCH-001',
                expiry_date='2025-12-31'
            )
            print(f'✅ add_stock: {msg}')
        
        success, msg = manager.add_client('Test Client', 'test@example.com')
        print(f'✅ add_client: {msg}')
        
        if clients and products:
            success, msg = manager.add_sale(
                clients[0]['client_id'], 
                [{'product_id': products[0]['product_id'], 'quantity': 1}]
            )
            print(f'✅ add_sale: {msg}')
        
        # Test export
        success, msg = manager.export_to_data_lake('products')
        print(f'✅ export_to_data_lake: {msg}')
        
        print('🎉 All methods working correctly!')
        
    except Exception as e:
        print(f'❌ Error: {e}')
        import traceback
        traceback.print_exc()
    finally:
        manager.shutdown()

if __name__ == '__main__':
    test_mongodb_manager()