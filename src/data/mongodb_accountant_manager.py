#!/usr/bin/env python3
"""
MongoDB Accountant Data Manager - NEW ARCHITECTURE
Replaces SQLite with MongoDB as the primary data source for accountant operations.
This is the new foundation for the MongoDB → Kafka → Spark → MongoDB pipeline.
"""

import sys
import os
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from pymongo import MongoClient, ReturnDocument
import logging
import pandas as pd

# Add src to path for direct execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MongoDBAccountantManager:
    """
    NEW MongoDB-based Accountant Data Manager.
    This class handles all accountant operations directly in MongoDB,
    eliminating the need for SQLite and providing a more scalable solution.
    """
    
    def __init__(self, config: Config):
        """Initialize MongoDB Accountant Manager with configuration."""
        self.config = config
        self.mongo_client = None
        self.db = None
        self._initialize_mongodb()
        
    def _initialize_mongodb(self) -> bool:
        """Initialize MongoDB connection and collections."""
        try:
            # Connect to MongoDB
            self.mongo_client = MongoClient(self.config.MONGO_URI)
            self.db = self.mongo_client[self.config.DB_NAME]
            
            # Create indexes for better performance
            self._create_indexes()
            
            logger.info("✅ MongoDB Accountant Manager initialized successfully")
            logger.info(f"🗄️  Using database: {self.config.DB_NAME}")
            logger.info(f"🌐 MongoDB URI: {self.config.MONGO_URI}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize MongoDB Accountant Manager: {e}")
            return False
    
    def _create_indexes(self):
        """Create indexes for better query performance."""
        try:
            # Products collection indexes
            self.db.products.create_index([('name', 1)])
            self.db.products.create_index([('category', 1)])
            
            # Clients collection indexes
            self.db.clients.create_index([('name', 1)])
            self.db.clients.create_index([('email', 1)])
            self.db.clients.create_index([('loyalty_tier', 1)])
            
            # Sales collection indexes
            self.db.sales.create_index([('client_id', 1)])
            self.db.sales.create_index([('timestamp', -1)])
            
            # Inventory collection indexes
            self.db.inventory.create_index([('product_id', 1)])
            self.db.inventory.create_index([('batch_no', 1)])
            self.db.inventory.create_index([('expiry_date', 1)])
            
            logger.info("✅ MongoDB indexes created successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to create MongoDB indexes: {e}")
    
    def _generate_uuid(self) -> str:
        """Generate a UUID string."""
        return str(uuid.uuid4())
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp as ISO format string."""
        return datetime.now().isoformat()
    
    # ====================== PRODUCTS METHODS ======================
    
    def add_product(self, name: str, category: str, buy_price: float,
                   sell_price: float, min_margin_threshold: float) -> Tuple[bool, str]:
        """
        Add a new product to MongoDB.
        
        Args:
            name: Product name
            category: Product category
            buy_price: Purchase price
            sell_price: Selling price
            min_margin_threshold: Minimum acceptable profit margin
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Validate inputs
            if not name or not category:
                return False, "Name and category are required"
            
            if buy_price <= 0 or sell_price <= 0:
                return False, "Prices must be positive values"
            
            if min_margin_threshold <= 0 or min_margin_threshold >= 100:
                return False, "Margin threshold must be between 0 and 100"
            
            # Create product document
            product = {
                'product_id': self._generate_uuid(),
                'name': name,
                'category': category,
                'buy_price': float(buy_price),
                'sell_price': float(sell_price),
                'min_margin_threshold': float(min_margin_threshold),
                'created_at': self._get_current_timestamp(),
                'updated_at': self._get_current_timestamp()
            }
            
            # Insert into MongoDB
            result = self.db.products.insert_one(product)
            
            logger.info(f"✅ Product added: {name} (ID: {product['product_id']})")
            return True, f"Product '{name}' added successfully!"
            
        except Exception as e:
            logger.error(f"❌ Failed to add product: {e}")
            return False, f"Failed to add product: {str(e)}"
    
    def get_all_products(self) -> List[Dict[str, Any]]:
        """Get all products from MongoDB."""
        try:
            products = list(self.db.products.find({}, {'_id': 0}))
            logger.info(f"📊 Retrieved {len(products)} products from MongoDB")
            return products
            
        except Exception as e:
            logger.error(f"❌ Failed to get products: {e}")
            return []
    
    def get_product_by_id(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Get a product by ID."""
        try:
            product = self.db.products.find_one({'product_id': product_id}, {'_id': 0})
            if product:
                logger.info(f"📊 Retrieved product: {product['name']}")
            return product
            
        except Exception as e:
            logger.error(f"❌ Failed to get product by ID: {e}")
            return None
    
    def update_product(self, product_id: str, **kwargs) -> Tuple[bool, str]:
        """Update a product in MongoDB."""
        try:
            # Prepare update data
            update_data = {}
            if 'name' in kwargs:
                update_data['name'] = kwargs['name']
            if 'category' in kwargs:
                update_data['category'] = kwargs['category']
            if 'buy_price' in kwargs:
                update_data['buy_price'] = float(kwargs['buy_price'])
            if 'sell_price' in kwargs:
                update_data['sell_price'] = float(kwargs['sell_price'])
            if 'min_margin_threshold' in kwargs:
                update_data['min_margin_threshold'] = float(kwargs['min_margin_threshold'])
            
            if not update_data:
                return False, "No update data provided"
            
            update_data['updated_at'] = self._get_current_timestamp()
            
            # Update product
            result = self.db.products.update_one(
                {'product_id': product_id},
                {'$set': update_data}
            )
            
            if result.modified_count == 1:
                logger.info(f"✅ Product updated: {product_id}")
                return True, "Product updated successfully!"
            else:
                logger.warning(f"⚠️  Product not found or not modified: {product_id}")
                return False, "Product not found or no changes made"
                
        except Exception as e:
            logger.error(f"❌ Failed to update product: {e}")
            return False, f"Failed to update product: {str(e)}"
    
    # ====================== INVENTORY METHODS ======================
    
    def add_inventory(self, product_id: str, quantity: int, batch_no: str,
                     expiry_date: str, location: str = "Main Warehouse") -> Tuple[bool, str]:
        """
        Add inventory for a product.
        
        Args:
            product_id: Product ID
            quantity: Quantity in stock
            batch_no: Batch number
            expiry_date: Expiry date (YYYY-MM-DD)
            location: Storage location
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Validate inputs
            if not product_id or not batch_no or not expiry_date:
                return False, "Product ID, batch number, and expiry date are required"
            
            if quantity <= 0:
                return False, "Quantity must be positive"
            
            # Check if product exists
            product = self.get_product_by_id(product_id)
            if not product:
                return False, "Product does not exist"
            
            # Create inventory document
            inventory = {
                'inventory_id': self._generate_uuid(),
                'product_id': product_id,
                'product_name': product['name'],
                'quantity': int(quantity),
                'batch_no': batch_no,
                'expiry_date': expiry_date,
                'location': location,
                'last_movement': self._get_current_timestamp(),
                'created_at': self._get_current_timestamp(),
                'updated_at': self._get_current_timestamp()
            }
            
            # Insert into MongoDB
            result = self.db.inventory.insert_one(inventory)
            
            logger.info(f"✅ Inventory added: {batch_no} (ID: {inventory['inventory_id']})")
            return True, f"Inventory for batch {batch_no} added successfully!"
            
        except Exception as e:
            logger.error(f"❌ Failed to add inventory: {e}")
            return False, f"Failed to add inventory: {str(e)}"
    
    def get_all_inventory(self) -> List[Dict[str, Any]]:
        """Get all inventory items from MongoDB."""
        try:
            inventory = list(self.db.inventory.find({}, {'_id': 0}))
            logger.info(f"📊 Retrieved {len(inventory)} inventory items from MongoDB")
            return inventory
            
        except Exception as e:
            logger.error(f"❌ Failed to get inventory: {e}")
            return []
    
    def get_all_stock(self) -> List[Dict[str, Any]]:
        """Get all stock items from MongoDB (alias for get_all_inventory)."""
        return self.get_all_inventory()
    
    def update_inventory_quantity(self, inventory_id: str, quantity_change: int) -> Tuple[bool, str]:
        """Update inventory quantity."""
        try:
            # Get current inventory
            inventory = self.db.inventory.find_one({'inventory_id': inventory_id})
            if not inventory:
                return False, "Inventory item not found"
            
            # Calculate new quantity
            new_quantity = inventory['quantity'] + quantity_change
            if new_quantity < 0:
                return False, "Quantity cannot be negative"
            
            # Update inventory
            result = self.db.inventory.update_one(
                {'inventory_id': inventory_id},
                {
                    '$set': {
                        'quantity': new_quantity,
                        'last_movement': self._get_current_timestamp(),
                        'updated_at': self._get_current_timestamp()
                    }
                }
            )
            
            if result.modified_count == 1:
                logger.info(f"✅ Inventory updated: {inventory_id} (new quantity: {new_quantity})")
                return True, f"Inventory updated successfully! New quantity: {new_quantity}"
            else:
                return False, "Failed to update inventory"
                
        except Exception as e:
            logger.error(f"❌ Failed to update inventory quantity: {e}")
            return False, f"Failed to update inventory: {str(e)}"
    
    # ====================== CLIENTS METHODS ======================
    
    def add_client(self, name: str, email: str = None, phone: str = None) -> Tuple[bool, str]:
        """
        Add a new client to MongoDB.
        
        Args:
            name: Client name
            email: Client email (optional)
            phone: Client phone (optional)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Validate inputs
            if not name:
                return False, "Client name is required"
            
            # Create client document
            client = {
                'client_id': self._generate_uuid(),
                'name': name,
                'email': email,
                'phone': phone,
                'total_revenue_generated': 0.0,
                'loyalty_tier': 'Standard',
                'created_at': self._get_current_timestamp(),
                'updated_at': self._get_current_timestamp()
            }
            
            # Insert into MongoDB
            result = self.db.clients.insert_one(client)
            
            logger.info(f"✅ Client added: {name} (ID: {client['client_id']})")
            return True, f"Client '{name}' added successfully!"
            
        except Exception as e:
            logger.error(f"❌ Failed to add client: {e}")
            return False, f"Failed to add client: {str(e)}"
    
    def get_all_clients(self) -> List[Dict[str, Any]]:
        """Get all clients from MongoDB."""
        try:
            clients = list(self.db.clients.find({}, {'_id': 0}))
            logger.info(f"📊 Retrieved {len(clients)} clients from MongoDB")
            return clients
            
        except Exception as e:
            logger.error(f"❌ Failed to get clients: {e}")
            return []
    
    def get_client_by_id(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Get a client by ID."""
        try:
            client = self.db.clients.find_one({'client_id': client_id}, {'_id': 0})
            if client:
                logger.info(f"📊 Retrieved client: {client['name']}")
            return client
            
        except Exception as e:
            logger.error(f"❌ Failed to get client by ID: {e}")
            return None
    
    def update_client_revenue(self, client_id: str, amount: float) -> Tuple[bool, str]:
        """Update client's total revenue."""
        try:
            # Update client revenue
            result = self.db.clients.update_one(
                {'client_id': client_id},
                {
                    '$inc': {'total_revenue_generated': float(amount)},
                    '$set': {'updated_at': self._get_current_timestamp()}
                }
            )
            
            if result.modified_count == 1:
                # Update loyalty tier based on revenue
                self._update_client_loyalty_tier(client_id)
                logger.info(f"✅ Client revenue updated: {client_id}")
                return True, "Client revenue updated successfully!"
            else:
                return False, "Client not found"
                
        except Exception as e:
            logger.error(f"❌ Failed to update client revenue: {e}")
            return False, f"Failed to update client revenue: {str(e)}"
    
    def _update_client_loyalty_tier(self, client_id: str):
        """Update client loyalty tier based on total revenue."""
        try:
            client = self.db.clients.find_one({'client_id': client_id})
            if not client:
                return
            
            revenue = client['total_revenue_generated']
            new_tier = 'Standard'
            
            if revenue > self.config.REVENUE_THRESHOLD_GOLD:
                new_tier = 'Gold'
            elif revenue > self.config.REVENUE_THRESHOLD_SILVER:
                new_tier = 'Silver'
            elif revenue > self.config.REVENUE_THRESHOLD_PROMO:
                new_tier = 'Bronze'
            
            if new_tier != client['loyalty_tier']:
                self.db.clients.update_one(
                    {'client_id': client_id},
                    {'$set': {'loyalty_tier': new_tier}}
                )
                logger.info(f"🏆 Client loyalty tier updated: {client_id} → {new_tier}")
                
        except Exception as e:
            logger.error(f"❌ Failed to update loyalty tier: {e}")
    
    # ====================== SALES METHODS ======================
    
    def add_sale(self, client_id: str, items: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Add a new sale to MongoDB.
        
        Args:
            client_id: Client ID
            items: List of sale items with product_id and quantity
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            if not client_id or not items:
                return False, "Client ID and sale items are required"
            
            if len(items) == 0:
                return False, "At least one sale item is required"
            
            # Check if client exists
            client = self.get_client_by_id(client_id)
            if not client:
                return False, "Client does not exist"
            
            # Validate all items and calculate total
            total_amount = 0.0
            sale_items = []
            
            for item in items:
                product_id = item.get('product_id')
                quantity = item.get('quantity')
                
                if not product_id or quantity is None:
                    return False, "Each item must have product_id and quantity"
                
                if quantity <= 0:
                    return False, "Quantity must be positive"
                
                # Get product price
                product = self.get_product_by_id(product_id)
                if not product:
                    return False, f"Product with ID {product_id} does not exist"
                
                unit_price = product['sell_price']
                item_total = unit_price * quantity
                total_amount += item_total
                
                sale_items.append({
                    'sales_item_id': self._generate_uuid(),
                    'product_id': product_id,
                    'product_name': product['name'],
                    'quantity': int(quantity),
                    'unit_price': float(unit_price),
                    'total_price': float(item_total)
                })
            
            # Create sale document
            sale = {
                'invoice_id': self._generate_uuid(),
                'client_id': client_id,
                'client_name': client['name'],
                'timestamp': self._get_current_timestamp(),
                'total_amount': float(total_amount),
                'items': sale_items,
                'created_at': self._get_current_timestamp()
            }
            
            # Insert sale into MongoDB
            sale_result = self.db.sales.insert_one(sale)
            
            # Update client revenue
            self.update_client_revenue(client_id, total_amount)
            
            logger.info(f"✅ Sale added: Invoice {sale['invoice_id']} for €{total_amount:.2f}")
            return True, f"Sale recorded successfully! Invoice ID: {sale['invoice_id']}"
            
        except Exception as e:
            logger.error(f"❌ Failed to add sale: {e}")
            return False, f"Failed to add sale: {str(e)}"
    
    def get_all_sales(self) -> List[Dict[str, Any]]:
        """Get all sales from MongoDB."""
        try:
            sales = list(self.db.sales.find({}, {'_id': 0}))
            logger.info(f"📊 Retrieved {len(sales)} sales from MongoDB")
            return sales
            
        except Exception as e:
            logger.error(f"❌ Failed to get sales: {e}")
            return []
    
    def get_sale_by_id(self, invoice_id: str) -> Optional[Dict[str, Any]]:
        """Get a sale by invoice ID."""
        try:
            sale = self.db.sales.find_one({'invoice_id': invoice_id}, {'_id': 0})
            if sale:
                logger.info(f"📊 Retrieved sale: {invoice_id}")
            return sale
            
        except Exception as e:
            logger.error(f"❌ Failed to get sale by ID: {e}")
            return None
    
    # ====================== KAFKA INTEGRATION METHODS ======================
    
    def get_data_for_kafka(self, collection_name: str) -> List[Dict[str, Any]]:
        """
        Get data from a MongoDB collection formatted for Kafka.
        
        Args:
            collection_name: Name of the MongoDB collection
            
        Returns:
            List of documents ready for Kafka production
        """
        try:
            # Check if collection exists
            if collection_name not in self.db.list_collection_names():
                logger.warning(f"⚠️  Collection '{collection_name}' not found")
                return []
            
            # Get the collection
            collection = self.db[collection_name]
            
            # Get all documents from the collection
            documents = list(collection.find({}, {'_id': 0}))
            
            logger.info(f"📊 Retrieved {len(documents)} records from {collection_name} for Kafka")
            return documents
            
        except Exception as e:
            logger.error(f"❌ Failed to get data for Kafka from {collection_name}: {e}")
            return []
    
    def get_products_for_kafka(self) -> List[Dict[str, Any]]:
        """Get products data formatted for Kafka inventory topic."""
        products = self.get_data_for_kafka('products')
        
        # Transform for Kafka inventory topic
        kafka_products = []
        for product in products:
            kafka_product = {
                'product_id': product['product_id'],
                'name': product['name'],
                # Keep 'name' field as it exists in MongoDB, Spark will alias it to 'product_name'
                'category': product['category'],
                'buy_price': product['buy_price'],
                'sell_price': product['sell_price'],
                'min_margin_threshold': product['min_margin_threshold'],
                'created_at': product['created_at'],
                'updated_at': product['updated_at'],
                'source': 'mongodb_accountant',
                'timestamp': self._get_current_timestamp()
            }
            kafka_products.append(kafka_product)
        
        return kafka_products
    
    def get_clients_for_kafka(self) -> List[Dict[str, Any]]:
        """Get clients data formatted for Kafka clients topic."""
        clients = self.get_data_for_kafka('clients')
        
        # Transform for Kafka clients topic
        kafka_clients = []
        for client in clients:
            kafka_client = {
                'client_id': client['client_id'],
                'name': client['name'],
                'email': client.get('email'),
                'phone': client.get('phone'),
                'total_revenue_generated': client.get('total_revenue_generated', 0.0),
                'loyalty_tier': client.get('loyalty_tier', 'Standard'),
                'source': 'mongodb_accountant',
                'timestamp': self._get_current_timestamp()
            }
            kafka_clients.append(kafka_client)
        
        return kafka_clients
    
    def get_sales_for_kafka(self) -> List[Dict[str, Any]]:
        """Get sales data formatted for Kafka sales topic."""
        sales = self.get_data_for_kafka('sales')
        
        # Transform for Kafka sales topic
        kafka_sales = []
        for sale in sales:
            kafka_sale = {
                'invoice_id': sale['invoice_id'],
                'client_id': sale['client_id'],
                'timestamp': sale['timestamp'],
                'total_amount': sale['total_amount'],
                'items': [
                    {
                        'product_id': item['product_id'],
                        'quantity': item['quantity']
                    } for item in sale['items']
                ],
                'source': 'mongodb_accountant',
                'processing_timestamp': self._get_current_timestamp()
            }
            kafka_sales.append(kafka_sale)
        
        return kafka_sales
    
    def get_inventory_for_kafka(self) -> List[Dict[str, Any]]:
        """Get inventory data formatted for Kafka inventory topic."""
        inventory = self.get_data_for_kafka('inventory')
        
        # Transform for Kafka inventory topic
        kafka_inventory = []
        for item in inventory:
            kafka_item = {
                'product_id': item['product_id'],
                'name': item.get('name', item.get('product_name', 'Unknown')),  # Handle missing name field
                'category': 'Unknown',  # Will be enriched by Spark
                'buy_price': 0.0,       # Will be enriched by Spark
                'sell_price': 0.0,      # Will be enriched by Spark
                'quantity': item['quantity'],
                'last_movement': item.get('last_movement', ''),  # Handle missing field
                'expiry_date': item.get('expiry_date', ''),      # Handle missing field
                'batch_no': item.get('batch_no', 'DEFAULT'),     # Handle missing field
                'source': 'mongodb_accountant',
                'timestamp': self._get_current_timestamp()
            }
            kafka_inventory.append(kafka_item)
        
        return kafka_inventory
    
    # ====================== DATA EXPORT METHODS ======================
    
    def export_to_data_lake(self, collection_name: str, output_dir: str = "/tmp/data_lake") -> Tuple[bool, str]:
        """
        Export collection data to Excel file in data lake.
        
        Args:
            collection_name: Name of the MongoDB collection
            output_dir: Directory to save Excel files
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Get data from MongoDB
            data = self.get_data_for_kafka(collection_name)
            if not data:
                return False, f"No data found in collection {collection_name}"
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{collection_name}_{timestamp}.xlsx"
            filepath = os.path.join(output_dir, filename)
            
            # Export to Excel
            df.to_excel(filepath, index=False)
            
            logger.info(f"✅ Exported {len(data)} records to {filepath}")
            return True, f"Data exported to {filepath}"
            
        except Exception as e:
            logger.error(f"❌ Failed to export to data lake: {e}")
            return False, f"Failed to export data: {str(e)}"
    
    # ====================== SHUTDOWN ======================
    
    def shutdown(self):
        """Cleanly shutdown the MongoDB Accountant Manager."""
        try:
            if self.mongo_client:
                self.mongo_client.close()
                self.mongo_client = None
                self.db = None
                logger.info("✅ MongoDB Accountant Manager shutdown completed")
        except Exception as e:
            logger.error(f"❌ Error during shutdown: {e}")


# Example usage and testing
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from utils.config import Config
    
    # Initialize
    config = Config()
    manager = MongoDBAccountantManager(config)
    
    # Test adding data
    print("🧪 Testing MongoDB Accountant Manager...")
    
    # Add test product
    success, msg = manager.add_product(
        name="Test Product",
        category="Electronics",
        buy_price=100.0,
        sell_price=150.0,
        min_margin_threshold=30.0
    )
    print(f"Product: {msg}")
    
    # Add test client
    success, msg = manager.add_client(
        name="Test Client",
        email="test@example.com"
    )
    print(f"Client: {msg}")
    
    # Get products for Kafka
    products = manager.get_products_for_kafka()
    print(f"Products for Kafka: {len(products)} items")
    
    # Get clients for Kafka
    clients = manager.get_clients_for_kafka()
    print(f"Clients for Kafka: {len(clients)} items")
    
    # Export to data lake
    success, msg = manager.export_to_data_lake('products')
    print(f"Export: {msg}")
    
    # Shutdown
    manager.shutdown()
    print("✅ Test completed successfully!")