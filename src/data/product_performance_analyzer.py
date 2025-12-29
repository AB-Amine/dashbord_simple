#!/usr/bin/env python3
"""
Product Performance Analyzer
Generates product performance data from existing sales and products
when Spark analytics haven't been run yet.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.mongodb_accountant_manager import MongoDBAccountantManager
from utils.config import Config
import pandas as pd
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ProductPerformanceAnalyzer")

class ProductPerformanceAnalyzer:
    def __init__(self):
        """Initialize product performance analyzer."""
        self.config = Config()
        self.manager = MongoDBAccountantManager(self.config)
        
    def get_product_performance_data(self) -> List[Dict[str, Any]]:
        """
        Get product performance data, either from product_winners collection
        or generate it from sales and products if not available.
        """
        try:
            # First try to get from product_winners (if Spark has run)
            product_winners = self.manager.get_data_for_kafka('product_winners')
            
            if product_winners:
                logger.info("📊 Using existing product_winners data from Spark analytics")
                return self._enrich_product_winners(product_winners)
            else:
                logger.info("📊 Generating product performance data from sales and products")
                return self._generate_product_performance_from_sales()
                
        except Exception as e:
            logger.error(f"❌ Error getting product performance: {e}")
            return []
    
    def _enrich_product_winners(self, product_winners: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich product_winners data with full product information.
        """
        try:
            # Get all products for enrichment
            products = self.manager.get_all_products()
            if not products:
                return product_winners
                
            # Create product lookup dictionary
            products_dict = {p['product_id']: p for p in products}
            
            # Enrich each product winner
            enriched_winners = []
            for winner in product_winners:
                product_id = winner.get('product_id')
                if product_id and product_id in products_dict:
                    product = products_dict[product_id]
                    
                    # Create enriched record
                    enriched = {
                        'product_id': product_id,
                        'product_name': product.get('name', 'Unknown'),
                        'category': product.get('category', 'Unknown'),
                        'total_sales': winner.get('total_sales', 0),
                        'total_revenue': winner.get('total_revenue', 0),
                        'profit_margin': winner.get('profit_margin', 0),
                        'buy_price': product.get('buy_price', 0),
                        'sell_price': product.get('sell_price', 0)
                    }
                    enriched_winners.append(enriched)
                else:
                    enriched_winners.append(winner)
            
            return enriched_winners
            
        except Exception as e:
            logger.error(f"❌ Error enriching product winners: {e}")
            return product_winners
    
    def _generate_product_performance_from_sales(self) -> List[Dict[str, Any]]:
        """
        Generate product performance data from sales and products.
        """
        try:
            # Get sales and products
            sales = self.manager.get_all_sales()
            products = self.manager.get_all_products()
            
            if not sales or not products:
                return []
            
            # Convert to DataFrames for easier analysis
            sales_df = pd.DataFrame(sales)
            products_df = pd.DataFrame(products)
            
            # Explode sales items to get individual product sales
            if 'items' in sales_df.columns:
                # Expand items into separate rows
                items_data = []
                for _, sale in sales_df.iterrows():
                    for item in sale['items']:
                        item_data = {
                            'sale_id': sale.get('invoice_id', ''),
                            'product_id': item.get('product_id', ''),
                            'quantity': item.get('quantity', 0),
                            'timestamp': sale.get('timestamp', '')
                        }
                        items_data.append(item_data)
                
                items_df = pd.DataFrame(items_data)
                
                # Merge with products to get product details
                if not items_df.empty:
                    performance_df = pd.merge(
                        items_df,
                        products_df,
                        on='product_id',
                        how='left'
                    )
                    
                    # Calculate performance metrics
                    if not performance_df.empty:
                        # Group by product and calculate totals
                        performance_summary = performance_df.groupby('product_id').agg({
                            'quantity': 'sum',
                            'name': 'first',
                            'category': 'first',
                            'buy_price': 'first',
                            'sell_price': 'first'
                        }).reset_index()
                        
                        # Calculate revenue and profit
                        performance_summary['total_revenue'] = (
                            performance_summary['quantity'] * performance_summary['sell_price']
                        )
                        performance_summary['total_cost'] = (
                            performance_summary['quantity'] * performance_summary['buy_price']
                        )
                        performance_summary['profit_margin'] = (
                            (performance_summary['total_revenue'] - performance_summary['total_cost']) / 
                            performance_summary['total_revenue']
                        ).fillna(0) * 100  # Convert to percentage
                        
                        # Rename columns for consistency
                        performance_summary = performance_summary.rename(columns={
                            'name': 'product_name',
                            'quantity': 'total_sales'
                        })
                        
                        # Convert to list of dicts
                        result = performance_summary.to_dict('records')
                        
                        logger.info(f"✅ Generated performance data for {len(result)} products")
                        return result
            
            return []
            
        except Exception as e:
            logger.error(f"❌ Error generating product performance: {e}")
            return []
    
    def get_top_performing_products(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get top performing products by revenue.
        """
        try:
            performance_data = self.get_product_performance_data()
            if not performance_data:
                return []
            
            # Convert to DataFrame for sorting
            df = pd.DataFrame(performance_data)
            
            # Sort by revenue and get top products
            top_products = df.nlargest(limit, 'total_revenue')
            
            return top_products.to_dict('records')
            
        except Exception as e:
            logger.error(f"❌ Error getting top products: {e}")
            return []
    
    def shutdown(self):
        """Cleanly shutdown the analyzer."""
        if self.manager:
            self.manager.shutdown()

def main():
    """Test the product performance analyzer."""
    analyzer = ProductPerformanceAnalyzer()
    
    try:
        # Test getting product performance data
        performance_data = analyzer.get_product_performance_data()
        print(f"📊 Product performance data: {len(performance_data)} records")
        
        if performance_data:
            print("\nTop 3 products:")
            for i, product in enumerate(performance_data[:3], 1):
                print(f"{i}. {product.get('product_name', 'Unknown')} - €{product.get('total_revenue', 0):.2f}")
        
        # Test getting top products
        top_products = analyzer.get_top_performing_products()
        print(f"\n🏆 Top performing products: {len(top_products)}")
        
    finally:
        analyzer.shutdown()

if __name__ == "__main__":
    main()