import pandas as pd
import sqlite3
from datetime import datetime, timedelta

def get_db_connection():
    conn = sqlite3.connect('stock_management.db')
    conn.row_factory = sqlite3.Row
    return conn

def calculate_product_winner(time_window='monthly'):
    conn = get_db_connection()
    sales_df = pd.read_sql_query("SELECT * FROM facture_details", conn)
    products_df = pd.read_sql_query("SELECT * FROM product", conn)
    conn.close()

    sales_df = sales_df.rename(columns={'price': 'sell_price'})
    merged_df = pd.merge(sales_df, products_df, left_on='product_id', right_on='id')
    
    # The 'price' column from the product table is the retail price
    # The 'buy_price' is the cost of goods sold.
    # The 'sell_price' from the facture_details is the price at the time of sale.
    merged_df['profit'] = (merged_df['sell_price'] - merged_df['buy_price']) * merged_df['quantity_x']
    
    # The logic from the PDF to calculate winner products
    # In the PDF, it seems that 'price' is 'sell_price'. Here I am using the sell_price from the invoice
    # and the buy_price from the product table.
    
    # Assuming 'date' is in the 'facture' table, we need to join with it.
    conn = get_db_connection()
    factures_df = pd.read_sql_query("SELECT * FROM facture", conn)
    conn.close()
    
    factures_df['date'] = pd.to_datetime(factures_df['date'])
    
    merged_df = pd.merge(merged_df, factures_df, left_on='facture_id', right_on='id')

    if time_window == 'monthly':
        start_date = datetime.now() - timedelta(days=30)
    elif time_window == 'weekly':
        start_date = datetime.now() - timedelta(days=7)
    else: # daily
        start_date = datetime.now() - timedelta(days=1)
        
    recent_sales = merged_df[merged_df['date'] >= start_date]
    
    if recent_sales.empty:
        return pd.DataFrame()

    product_profit = recent_sales.groupby('product_id')['profit'].sum().reset_index()
    product_profit = product_profit.rename(columns={'profit': 'total_profit'})
    
    top_10_winners = product_profit.sort_values(by='total_profit', ascending=False).head(10)
    
    # get product name
    top_10_winners = pd.merge(top_10_winners, products_df[['id', 'name']], left_on='product_id', right_on='id')
    
    return top_10_winners[['name', 'total_profit']]


def detect_loss_products():
    conn = get_db_connection()
    stock_df = pd.read_sql_query("SELECT * FROM product", conn)
    conn.close()

    current_date = datetime.now()
    
    # Convert 'expiry_date' and 'last_movement' to datetime objects
    stock_df['expiry_date'] = pd.to_datetime(stock_df['expiry_date'], errors='coerce')
    stock_df['last_movement'] = pd.to_datetime(stock_df['last_movement'], errors='coerce')
    
    # Condition 1: Expiry date is within 7 days
    condition_expiring_soon = stock_df['expiry_date'] < (current_date + timedelta(days=7))
    
    # Condition 2: No movement (sales) in 30 days
    condition_no_movement = stock_df['last_movement'] < (current_date - timedelta(days=30))
    
    # Combine conditions with OR, similar to the PDF's pseudo-code
    loss_products = stock_df[condition_expiring_soon | condition_no_movement]
    
    return loss_products[['name', 'quantity', 'expiry_date', 'last_movement']]

def apply_client_reduction():
    conn = get_db_connection()
    clients_df = pd.read_sql_query("SELECT * FROM client", conn)
    conn.close()
    
    REVENUE_THRESHOLD_GOLD = 20000
    REVENUE_THRESHOLD_SILVER = 10000

    def assign_tier(revenue):
        if revenue >= REVENUE_THRESHOLD_GOLD:
            return 'Gold'
        elif revenue >= REVENUE_THRESHOLD_SILVER:
            return 'Silver'
        else:
            return 'Standard'

    clients_df['new_loyalty_tier'] = clients_df['total_revenue_generated'].apply(assign_tier)
    
    # update client loyalty tier in db
    conn = get_db_connection()
    cursor = conn.cursor()
    for index, row in clients_df.iterrows():
        if row['loyalty_tier'] != row['new_loyalty_tier']:
             cursor.execute('UPDATE client SET loyalty_tier = ? WHERE id = ?', (row['new_loyalty_tier'], row['id']))
    conn.commit()
    conn.close()
    
    # Return clients with changed loyalty tier
    updated_clients = clients_df[clients_df['loyalty_tier'] != clients_df['new_loyalty_tier']]

    return updated_clients[['name', 'total_revenue_generated', 'loyalty_tier', 'new_loyalty_tier']]

if __name__ == '__main__':
    # for testing purposes
    print("Product Winners (Monthly):")
    print(calculate_product_winner('monthly'))
    print("\nLoss Products:")
    print(detect_loss_products())
    print("\nClient Reductions:")
    print(apply_client_reduction())
