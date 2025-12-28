#!/usr/bin/env python3
"""
Data Manager class for handling SQLite database operations.
"""

import sqlite3
import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataManager:
    """
    Class responsible for managing SQLite database operations.
    """
    
    def __init__(self, config: Config):
        """Initialize DataManager with configuration."""
        self.config = config
        self.connection = None
    
    def initialize_database(self) -> bool:
        """Initialize the SQLite database with required tables."""
        try:
            self.connection = sqlite3.connect(self.config.SQLITE_DB_PATH)
            cursor = self.connection.cursor()
            
            # Create product table (old schema for backward compatibility)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS product (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    category TEXT,
                    buy_price REAL,
                    min_margin_threshold REAL,
                    last_movement TEXT,
                    expiry_date TEXT,
                    batch_no TEXT
                )
            ''')
            
            # Create client table (old schema for backward compatibility)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS client (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT,
                    phone TEXT,
                    total_revenue_generated REAL DEFAULT 0,
                    loyalty_tier TEXT DEFAULT 'Standard'
                )
            ''')
            
            # Create facture table (invoice) - old schema for backward compatibility
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS facture (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    total REAL NOT NULL,
                    FOREIGN KEY (client_id) REFERENCES client (id)
                )
            ''')
            
            # Create facture_details table to link products to factures - old schema
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS facture_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    facture_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    FOREIGN KEY (facture_id) REFERENCES facture (id),
                    FOREIGN KEY (product_id) REFERENCES product (id)
                )
            ''')
            
            # Create indexes for better performance on old tables
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_facture_client ON facture(client_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_facture_details_facture ON facture_details(facture_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_facture_details_product ON facture_details(product_id)')
            
            self.connection.commit()
            logger.info("Database initialization completed successfully")
            return True
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()
    
    def get_connection(self):
        """Get a database connection."""
        if not self.connection:
            self.connection = sqlite3.connect(self.config.SQLITE_DB_PATH)
        return self.connection
    
    def load_excel_data(self, file_path: str) -> bool:
        """Load data from Excel file into SQLite database."""
        try:
            # Read Excel sheets
            products_df = pd.read_excel(file_path, sheet_name='Products')
            clients_df = pd.read_excel(file_path, sheet_name='Clients')
            invoices_df = pd.read_excel(file_path, sheet_name='Invoices')
            invoice_details_df = pd.read_excel(file_path, sheet_name='Invoice_Details')
            
            conn = self.get_connection()
            
            # Load products
            products_df.to_sql('product', conn, if_exists='replace', index=False)
            
            # Load clients
            clients_df.to_sql('client', conn, if_exists='replace', index=False)
            
            # Load invoices
            invoices_df.to_sql('facture', conn, if_exists='replace', index=False)
            
            # Load invoice details
            invoice_details_df.to_sql('facture_details', conn, if_exists='replace', index=False)
            
            conn.commit()
            logger.info(f"✅ Successfully loaded Excel data from {file_path}")
            return True
        except FileNotFoundError:
            logger.error(f"❌ Excel file not found: {file_path}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to load Excel data: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()
    
    def get_products(self) -> List[Dict[str, Any]]:
        """Get all products from database."""
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM product", conn)
            return df.to_dict('records')
        except Exception as e:
            print(f"Failed to get products: {e}")
            return []
    
    def get_clients(self) -> List[Dict[str, Any]]:
        """Get all clients from database."""
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM client", conn)
            return df.to_dict('records')
        except Exception as e:
            print(f"Failed to get clients: {e}")
            return []
    
    def get_invoices(self) -> List[Dict[str, Any]]:
        """Get all invoices from database."""
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM facture", conn)
            return df.to_dict('records')
        except Exception as e:
            print(f"Failed to get invoices: {e}")
            return []
    
    def get_invoice_details(self) -> List[Dict[str, Any]]:
        """Get all invoice details from database."""
        try:
            conn = self.get_connection()
            df = pd.read_sql_query("SELECT * FROM facture_details", conn)
            return df.to_dict('records')
        except Exception as e:
            print(f"Failed to get invoice details: {e}")
            return []
        finally:
            if self.connection:
                self.connection.close()
    
    def shutdown(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None