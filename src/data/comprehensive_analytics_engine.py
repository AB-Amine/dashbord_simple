#!/usr/bin/env python3
"""
Comprehensive Analytics Engine
Generates and stores complete analytics data including:
- Product performance (winners)
- Loss risk products
- Client performance
- Sales analytics
- Inventory risk analysis
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from utils.config import Config
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ComprehensiveAnalyticsEngine")

class ComprehensiveAnalyticsEngine:
    def __init__(self):
        """Initialize comprehensive analytics engine."""
        self.config = Config()
        self.manager = MongoDBAccountantManager(self.config)
        
    def run_complete_analytics(self) -> bool:
        """Run complete analytics and store results in MongoDB."""
        try:
            logger.info("🚀 Starting comprehensive analytics processing...")
            
            # Run all analytics
            success_count = 0
            
            # 1. Product performance analytics
            if self._generate_and_store_product_winners():
                success_count += 1
            
            # 2. Loss risk products analytics
            if self._generate_and_store_loss_risk_products():
                success_count += 1
            
            # 3. Client performance analytics
            if self._generate_and_store_client_performance():
                success_count += 1
            
            # 4. Sales analytics
            if self._generate_and_store_sales_analytics():
                success_count += 1
            
            logger.info(f"✅ Completed {success_count}/4 analytics processes")
            return success_count == 4
            
        except Exception as e:
            logger.error(f"❌ Analytics engine failed: {e}")
            return False
        finally:
            self.manager.shutdown()
    
    def _generate_and_store_product_winners(self) -> bool:
        """Generate and store product winners data."""
        try:
            logger.info("📊 Generating product winners analytics...")
            
            # Get sales and products data
            sales = self.manager.get_all_sales()
            products = self.manager.get_all_products()
            
            if not sales or not products:
                logger.warning("⚠️  Not enough data for product winners analytics")
                return False
            
            # Convert to DataFrames
            sales_df = pd.DataFrame(sales)
            products_df = pd.DataFrame(products)
            
            # Explode sales items
            items_data = []
            for _, sale in sales_df.iterrows():
                for item in sale.get('items', []):
                    items_data.append({
                        'sale_id': sale.get('invoice_id', ''),
                        'product_id': item.get('product_id', ''),
                        'quantity': item.get('quantity', 0),
                        'timestamp': sale.get('timestamp', '')
                    })
            
            if not items_data:
                logger.warning("⚠️  No sales items found")
                return False
            
            items_df = pd.DataFrame(items_data)
            
            # Merge with products and calculate performance
            performance_df = pd.merge(items_df, products_df, on='product_id', how='left')
            
            # Calculate metrics
            performance_summary = performance_df.groupby('product_id').agg({
                'quantity': 'sum',
                'name': 'first',
                'category': 'first',
                'buy_price': 'first',
                'sell_price': 'first'
            }).reset_index()
            
            # Calculate financial metrics
            performance_summary['total_revenue'] = performance_summary['quantity'] * performance_summary['sell_price']
            performance_summary['total_cost'] = performance_summary['quantity'] * performance_summary['buy_price']
            performance_summary['total_profit'] = performance_summary['total_revenue'] - performance_summary['total_cost']
            performance_summary['profit_margin'] = (
                (performance_summary['total_profit'] / performance_summary['total_revenue']).fillna(0) * 100
            )
            
            # Rank products by revenue
            performance_summary['ranking'] = performance_summary['total_revenue'].rank(ascending=False, method='min')
            
            # Prepare for MongoDB storage
            product_winners = performance_summary.to_dict('records')
            
            # Store in MongoDB
            for winner in product_winners:
                # Check if product_winners collection exists, create if not
                if not self._collection_exists('product_winners'):
                    self._create_collection('product_winners')
                
                # Store the winner data
                self.manager.db.product_winners.update_one(
                    {'product_id': winner['product_id']},
                    {'$set': winner},
                    upsert=True
                )
            
            logger.info(f"✅ Stored {len(product_winners)} product winners")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to generate product winners: {e}")
            return False
    
    def _generate_and_store_loss_risk_products(self) -> bool:
        """Generate and store loss risk products data."""
        try:
            logger.info("⚠️  Generating loss risk products analytics...")
            
            # Get inventory data
            inventory = self.manager.get_all_inventory()
            products = self.manager.get_all_products()
            
            if not inventory or not products:
                logger.warning("⚠️  Not enough data for loss risk analytics")
                return False
            
            inventory_df = pd.DataFrame(inventory)
            products_df = pd.DataFrame(products)
            
            # Convert timestamp strings to datetime for analysis
            if 'expiry_date' in inventory_df.columns:
                inventory_df['expiry_date_dt'] = pd.to_datetime(inventory_df['expiry_date'], errors='coerce')
            
            if 'last_movement' in inventory_df.columns:
                inventory_df['last_movement_dt'] = pd.to_datetime(inventory_df['last_movement'], errors='coerce')
            
            # Calculate risk factors
            today = datetime.now()
            
            # Identify products at risk
            risk_products = []
            
            for _, item in inventory_df.iterrows():
                risk_level = 'Low'
                risk_reason = 'None'
                potential_loss = 0
                
                # Check expiry risk (expiring within 30 days)
                expiry_date = item.get('expiry_date_dt')
                if pd.notna(expiry_date):
                    days_until_expiry = (expiry_date - today).days
                    if days_until_expiry < 7:
                        risk_level = 'High'
                        risk_reason = 'Expiring soon'
                    elif days_until_expiry < 30:
                        risk_level = 'Medium'
                        risk_reason = 'Expiring within 30 days'
                
                # Check dead stock risk (no movement in 30+ days)
                last_movement = item.get('last_movement_dt')
                if pd.notna(last_movement):
                    days_since_movement = (today - last_movement).days
                    if days_since_movement > 30:
                        risk_level = 'High' if risk_level == 'High' else 'Medium'
                        risk_reason += ', Dead stock' if risk_reason else 'Dead stock'
                
                # Calculate potential loss
                product_id = item.get('product_id')
                quantity = item.get('quantity', 0)
                
                # Find product price
                product = next((p for p in products if p['product_id'] == product_id), None)
                if product:
                    buy_price = product.get('buy_price', 0)
                    potential_loss = quantity * buy_price
                
                if risk_level != 'Low':
                    risk_products.append({
                        'product_id': product_id,
                        'product_name': item.get('product_name'),
                        'category': item.get('category'),
                        'quantity': quantity,
                        'potential_loss': potential_loss,
                        'risk_level': risk_level,
                        'risk_reason': risk_reason,
                        'days_until_expiry': days_until_expiry if 'days_until_expiry' in locals() else None,
                        'days_since_movement': days_since_movement if 'days_since_movement' in locals() else None
                    })
            
            # Store in MongoDB
            if risk_products:
                if not self._collection_exists('loss_risk_products'):
                    self._create_collection('loss_risk_products')
                
                for risk_product in risk_products:
                    self.manager.db.loss_risk_products.update_one(
                        {'product_id': risk_product['product_id']},
                        {'$set': risk_product},
                        upsert=True
                    )
                
                logger.info(f"✅ Stored {len(risk_products)} loss risk products")
                return True
            else:
                logger.info("✅ No products at risk found")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to generate loss risk products: {e}")
            return False
    
    def _generate_and_store_client_performance(self) -> bool:
        """Generate and store client performance data."""
        try:
            logger.info("👥 Generating client performance analytics...")
            
            # Get clients and sales data
            clients = self.manager.get_all_clients()
            sales = self.manager.get_all_sales()
            
            if not clients or not sales:
                logger.warning("⚠️  Not enough data for client performance analytics")
                return False
            
            clients_df = pd.DataFrame(clients)
            sales_df = pd.DataFrame(sales)
            
            # Calculate client performance metrics
            client_sales = sales_df.groupby('client_id').agg({
                'total_amount': ['sum', 'count', 'mean'],
                'timestamp': 'count'
            }).reset_index()
            
            client_sales.columns = ['client_id', 'total_revenue', 'total_orders', 'avg_order_value', 'order_count']
            
            # Merge with client data
            client_performance = pd.merge(client_sales, clients_df, on='client_id', how='left')
            
            # Calculate additional metrics
            client_performance['revenue_per_order'] = client_performance['total_revenue'] / client_performance['total_orders']
            
            # Store in MongoDB (update client records with performance data)
            for _, client in client_performance.iterrows():
                client_id = client['client_id']
                
                # Update client with performance data
                self.manager.db.clients.update_one(
                    {'client_id': client_id},
                    {'$set': {
                        'total_revenue_generated': float(client['total_revenue']),
                        'total_orders': int(client['total_orders']),
                        'avg_order_value': float(client['avg_order_value']),
                        'revenue_per_order': float(client['revenue_per_order'])
                    }}
                )
            
            logger.info(f"✅ Updated {len(client_performance)} clients with performance data")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to generate client performance: {e}")
            return False
    
    def _generate_and_store_sales_analytics(self) -> bool:
        """Generate and store comprehensive sales analytics."""
        try:
            logger.info("💰 Generating sales analytics...")
            
            # Get sales data
            sales = self.manager.get_all_sales()
            
            if not sales:
                logger.warning("⚠️  No sales data available")
                return False
            
            sales_df = pd.DataFrame(sales)
            
            # Calculate overall sales metrics
            total_revenue = sales_df['total_amount'].sum()
            total_orders = len(sales_df)
            avg_order_value = sales_df['total_amount'].mean()
            
            # Calculate time-based metrics
            if 'timestamp' in sales_df.columns:
                sales_df['timestamp_dt'] = pd.to_datetime(sales_df['timestamp'], errors='coerce')
                sales_df['date'] = sales_df['timestamp_dt'].dt.date
                
                # Daily sales trends
                daily_sales = sales_df.groupby('date').agg({
                    'total_amount': 'sum',
                    'invoice_id': 'count'
                }).reset_index()
                daily_sales.columns = ['date', 'daily_revenue', 'daily_orders']
                
                # Store daily sales in MongoDB
                if not self._collection_exists('sales_analytics'):
                    self._create_collection('sales_analytics')
                
                # Store overall metrics
                self.manager.db.sales_analytics.update_one(
                    {'type': 'overall'},
                    {'$set': {
                        'total_revenue': float(total_revenue),
                        'total_orders': int(total_orders),
                        'avg_order_value': float(avg_order_value),
                        'last_updated': datetime.now().isoformat()
                    }},
                    upsert=True
                )
                
                # Store daily metrics
                for _, day in daily_sales.iterrows():
                    self.manager.db.sales_analytics.update_one(
                        {'type': 'daily', 'date': str(day['date'])},
                        {'$set': {
                            'daily_revenue': float(day['daily_revenue']),
                            'daily_orders': int(day['daily_orders']),
                            'date': str(day['date'])
                        }},
                        upsert=True
                    )
            
            logger.info("✅ Stored comprehensive sales analytics")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to generate sales analytics: {e}")
            return False
    
    def _collection_exists(self, collection_name: str) -> bool:
        """Check if a collection exists in MongoDB."""
        return collection_name in self.manager.db.list_collection_names()
    
    def _create_collection(self, collection_name: str):
        """Create a new collection in MongoDB."""
        self.manager.db.create_collection(collection_name)
        logger.info(f"✅ Created collection: {collection_name}")
    
    def shutdown(self):
        """Cleanly shutdown the analytics engine."""
        if self.manager:
            self.manager.shutdown()

def main():
    """Test the comprehensive analytics engine."""
    engine = ComprehensiveAnalyticsEngine()
    
    try:
        success = engine.run_complete_analytics()
        if success:
            print("🎉 Comprehensive analytics completed successfully!")
        else:
            print("⚠️  Some analytics processes failed")
    finally:
        engine.shutdown()

if __name__ == "__main__":
    main()