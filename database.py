import sqlite3

def init_db():
    conn = sqlite3.connect('stock_management.db')
    cursor = conn.cursor()

    # Create product table
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

    # Create client table
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

    # Create facture table (invoice)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facture (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total REAL NOT NULL,
            FOREIGN KEY (client_id) REFERENCES client (id)
        )
    ''')
    
    # Create facture_details table to link products to factures
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


    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_db()
