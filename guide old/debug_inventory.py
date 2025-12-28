#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

config = Config()
manager = MongoDBAccountantManager(config)
inventory = manager.get_inventory_for_kafka()
print(f'Inventory items: {len(inventory)}')
if inventory:
    print(f'First item keys: {list(inventory[0].keys())}')
    print(f'First item name: {inventory[0].get("name", "MISSING")}')
    print(f'First item: {inventory[0]}')
manager.shutdown()