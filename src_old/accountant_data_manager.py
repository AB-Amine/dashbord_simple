#!/usr/bin/env python3
"""
Accountant Data Manager class for handling SQLite database operations specifically for the Accountant Interface.
This class provides CRUD operations for manual data entry by accountants.
"""

import sqlite3
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from utils.config import Config
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AccountantDataManager:
    """
    Class responsible for managing SQLite database operations for the Accountant Interface.
    Provides methods for manual data entry and retrieval.
    """
    
    def __init__(self, config: Config):
        """Initialize AccountantDataManager with configuration."""
        self.config = config
        self.connection = None
        self._initialize_database()
    
    def _initialize_database(self) -> bool:
        """Initialize the SQLite database with required tables for accountant operations."""
        try:
            self.connection = sqlite3.connect(self.config.SQLITE_DB_PATH)
            cursor = self.connection.cursor()
            
            # Create Products table (Master Data)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    product_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    buy_price REAL NOT NULL,
                    sell_price REAL NOT NULL,
                    min_margin_threshold REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create Stock table (Inventory)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS stock (
                    stock_id TEXT PRIMARY KEY,
                    product_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    expiry_date TEXT NOT NULL,
                    last_movement TEXT DEFAULT CURRENT_TIMESTAMP,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products (product_id)
                )
            ''')
            
            # Create Clients table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS clients (
                    client_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    phone TEXT,
                    total_revenue_generated REAL DEFAULT 0,
                    loyalty_tier TEXT DEFAULT 'Standard',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create Sales/Facture table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sales (
                    invoice_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    total_amount REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (client_id) REFERENCES clients (client_id)
                )
            ''')
            
            # Create Sales Items table (for line items)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sales_items (
                    sales_item_id TEXT PRIMARY KEY,
                    invoice_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_price REAL NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (invoice_id) REFERENCES sales (invoice_id),
                    FOREIGN KEY (product_id) REFERENCES products (product_id)
                )
            ''')
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_product ON stock(product_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_client ON sales(client_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_items_invoice ON sales_items(invoice_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_items_product ON sales_items(product_id)')
            
            self.connection.commit()
            logger.info("Accountant database tables initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Accountant database initialization failed: {e}")
            return False
    
    def get_connection(self):
        """Get a database connection."""
        try:
            if not self.connection:
                self.connection = sqlite3.connect(self.config.SQLITE_DB_PATH)
                self.connection.execute('PRAGMA foreign_keys = ON')
            return self.connection
        except sqlite3.Error as e:
            logger.error(f"Failed to get database connection: {e}")
            if self.connection:
                self.connection.close()
                self.connection = None
            raise
    
    def _generate_uuid(self) -> str:
        """Generate a UUID string."""
        return str(uuid.uuid4())
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp as string."""
        return datetime.now().isoformat()
    
    # ====================== PRODUCTS METHODS ======================
    
    def add_product(self, name: str, category: str, buy_price: float, 
                   sell_price: float, min_margin_threshold: float) -> Tuple[bool, str]:
        """
        Add a new product to the database.
        
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
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Validate inputs
            if not name or not category:
                return False, "Name and category are required"
            
            if buy_price <= 0 or sell_price <= 0:
                return False, "Prices must be positive values"
            
            if min_margin_threshold <= 0 or min_margin_threshold >= 100:
                return False, "Margin threshold must be between 0 and 100"
            
            product_id = self._generate_uuid()
            timestamp = self._get_current_timestamp()
            
            cursor.execute('''
                INSERT INTO products (product_id, name, category, buy_price, sell_price, 
                                    min_margin_threshold, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (product_id, name, category, buy_price, sell_price, min_margin_threshold, 
                  timestamp, timestamp))
            
            conn.commit()
            logger.info(f"Product added successfully: {name} (ID: {product_id})")
            return True, f"Product '{name}' added successfully!"
            
        except Exception as e:
            logger.error(f"Failed to add product: {e}")
            return False, f"Failed to add product: {str(e)}"
    
    def get_all_products(self) -> List[Dict[str, Any]]:
        """Get all products from the database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT product_id, name, category, buy_price, sell_price, 
                       min_margin_threshold, created_at, updated_at
                FROM products
                ORDER BY name ASC
            ''')
            
            products = []
            for row in cursor.fetchall():
                products.append({
                    'product_id': row[0],
                    'name': row[1],
                    'category': row[2],
                    'buy_price': row[3],
                    'sell_price': row[4],
                    'min_margin_threshold': row[5],
                    'created_at': row[6],
                    'updated_at': row[7]
                })
            
            return products
            
        except Exception as e:
            logger.error(f"Failed to get products: {e}")
            return []
    
    # ====================== STOCK METHODS ======================
    
    def add_stock(self, product_id: str, quantity: int, batch_no: str, 
                 expiry_date: str) -> Tuple[bool, str]:
        """
        Add stock inventory for a product.
        
        Args:
            product_id: Product ID
            quantity: Quantity in stock
            batch_no: Batch number
            expiry_date: Expiry date (YYYY-MM-DD)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Validate inputs
            if not product_id or not batch_no or not expiry_date:
                return False, "Product ID, batch number, and expiry date are required"
            
            if quantity <= 0:
                return False, "Quantity must be positive"
            
            # Check if product exists
            cursor.execute('SELECT COUNT(*) FROM products WHERE product_id = ?', (product_id,))
            if cursor.fetchone()[0] == 0:
                return False, "Product does not exist"
            
            # Validate date format
            try:
                datetime.strptime(expiry_date, '%Y-%m-%d')
            except ValueError:
                return False, "Expiry date must be in YYYY-MM-DD format"
            
            stock_id = self._generate_uuid()
            timestamp = self._get_current_timestamp()
            
            cursor.execute('''
                INSERT INTO stock (stock_id, product_id, quantity, batch_no, expiry_date, 
                                 last_movement, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (stock_id, product_id, quantity, batch_no, expiry_date, timestamp, timestamp, timestamp))
            
            conn.commit()
            logger.info(f"Stock added successfully: {batch_no} (ID: {stock_id})")
            return True, f"Stock for batch {batch_no} added successfully!"
            
        except Exception as e:
            logger.error(f"Failed to add stock: {e}")
            return False, f"Failed to add stock: {str(e)}"
    
    def get_all_stock(self) -> List[Dict[str, Any]]:
        """Get all stock items from the database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT s.stock_id, s.product_id, p.name as product_name, 
                       s.quantity, s.batch_no, s.expiry_date, s.last_movement
                FROM stock s
                JOIN products p ON s.product_id = p.product_id
                ORDER BY p.name, s.expiry_date ASC
            ''')
            
            stock_items = []
            for row in cursor.fetchall():
                stock_items.append({
                    'stock_id': row[0],
                    'product_id': row[1],
                    'product_name': row[2],
                    'quantity': row[3],
                    'batch_no': row[4],
                    'expiry_date': row[5],
                    'last_movement': row[6]
                })
            
            return stock_items
            
        except Exception as e:
            logger.error(f"Failed to get stock: {e}")
            return []
    
    # ====================== CLIENTS METHODS ======================
    
    def add_client(self, name: str, email: str = None, phone: str = None) -> Tuple[bool, str]:
        """
        Add a new client to the database.
        
        Args:
            name: Client name
            email: Client email (optional)
            phone: Client phone (optional)
            
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Validate inputs
            if not name:
                return False, "Client name is required"
            
            client_id = self._generate_uuid()
            timestamp = self._get_current_timestamp()
            
            cursor.execute('''
                INSERT INTO clients (client_id, name, email, phone, 
                                    total_revenue_generated, loyalty_tier, 
                                    created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (client_id, name, email, phone, 0.0, 'Standard', timestamp, timestamp))
            
            conn.commit()
            logger.info(f"Client added successfully: {name} (ID: {client_id})")
            return True, f"Client '{name}' added successfully!"
            
        except Exception as e:
            logger.error(f"Failed to add client: {e}")
            return False, f"Failed to add client: {str(e)}"
    
    def get_all_clients(self) -> List[Dict[str, Any]]:
        """Get all clients from the database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT client_id, name, email, phone, 
                       total_revenue_generated, loyalty_tier, created_at
                FROM clients
                ORDER BY name ASC
            ''')
            
            clients = []
            for row in cursor.fetchall():
                clients.append({
                    'client_id': row[0],
                    'name': row[1],
                    'email': row[2],
                    'phone': row[3],
                    'total_revenue_generated': row[4],
                    'loyalty_tier': row[5],
                    'created_at': row[6]
                })
            
            return clients
            
        except Exception as e:
            logger.error(f"Failed to get clients: {e}")
            return []
    
    # ====================== SALES METHODS ======================
    
    def add_sale(self, client_id: str, items: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """
        Add a new sale/invoice to the database.
        
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
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Check if client exists
            cursor.execute('SELECT COUNT(*) FROM clients WHERE client_id = ?', (client_id,))
            if cursor.fetchone()[0] == 0:
                return False, "Client does not exist"
            
            # Validate all items
            total_amount = 0.0
            for item in items:
                product_id = item.get('product_id')
                quantity = item.get('quantity')
                
                if not product_id or quantity is None:
                    return False, "Each item must have product_id and quantity"
                
                if quantity <= 0:
                    return False, "Quantity must be positive"
                
                # Check if product exists
                cursor.execute('SELECT sell_price FROM products WHERE product_id = ?', (product_id,))
                result = cursor.fetchone()
                if not result:
                    return False, f"Product with ID {product_id} does not exist"
                
                unit_price = result[0]
                total_amount += unit_price * quantity
            
            # Create the sale/invoice
            invoice_id = self._generate_uuid()
            timestamp = self._get_current_timestamp()
            
            cursor.execute('''
                INSERT INTO sales (invoice_id, client_id, timestamp, total_amount, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (invoice_id, client_id, timestamp, total_amount, timestamp))
            
            # Add sale items
            for item in items:
                sales_item_id = self._generate_uuid()
                product_id = item['product_id']
                quantity = item['quantity']
                
                # Get product price
                cursor.execute('SELECT sell_price FROM products WHERE product_id = ?', (product_id,))
                unit_price = cursor.fetchone()[0]
                
                cursor.execute('''
                    INSERT INTO sales_items (sales_item_id, invoice_id, product_id, 
                                           quantity, unit_price, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (sales_item_id, invoice_id, product_id, quantity, unit_price, timestamp))
            
            # Update client's total revenue
            cursor.execute('''
                UPDATE clients 
                SET total_revenue_generated = total_revenue_generated + ?,
                    updated_at = ?
                WHERE client_id = ?
            ''', (total_amount, timestamp, client_id))
            
            conn.commit()
            logger.info(f"Sale added successfully: Invoice {invoice_id} for client {client_id}")
            return True, f"Sale recorded successfully! Invoice ID: {invoice_id}"
            
        except Exception as e:
            logger.error(f"Failed to add sale: {e}")
            return False, f"Failed to add sale: {str(e)}"
    
    def get_all_sales(self) -> List[Dict[str, Any]]:
        """Get all sales from the database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT s.invoice_id, s.client_id, c.name as client_name,
                       s.timestamp, s.total_amount
                FROM sales s
                JOIN clients c ON s.client_id = c.client_id
                ORDER BY s.timestamp DESC
            ''')
            
            sales = []
            for row in cursor.fetchall():
                sales.append({
                    'invoice_id': row[0],
                    'client_id': row[1],
                    'client_name': row[2],
                    'timestamp': row[3],
                    'total_amount': row[4]
                })
            
            return sales
            
        except Exception as e:
            logger.error(f"Failed to get sales: {e}")
            return []
    
    def get_sale_details(self, invoice_id: str) -> List[Dict[str, Any]]:
        """Get details for a specific sale/invoice."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT si.sales_item_id, si.product_id, p.name as product_name,
                       si.quantity, si.unit_price, (si.quantity * si.unit_price) as total_price
                FROM sales_items si
                JOIN products p ON si.product_id = p.product_id
                WHERE si.invoice_id = ?
            ''', (invoice_id,))
            
            items = []
            for row in cursor.fetchall():
                items.append({
                    'sales_item_id': row[0],
                    'product_id': row[1],
                    'product_name': row[2],
                    'quantity': row[3],
                    'unit_price': row[4],
                    'total_price': row[5]
                })
            
            return items
            
        except Exception as e:
            logger.error(f"Failed to get sale details: {e}")
            return []
    
    def shutdown(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None