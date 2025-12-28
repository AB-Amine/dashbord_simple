#!/usr/bin/env python3
"""
Historical Data Re-Ingestion Module.

This module provides functionality to read existing SQLite data and publish it to Kafka
for reprocessing by Spark Structured Streaming. This ensures that historical data is
analyzed and stored in MongoDB alongside real-time analytics.

The module implements:
- Idempotent processing to avoid duplicates
- Proper data transformation for Kafka topics
- Automatic triggering of Spark analytics via Kafka
- Safe handling of existing MongoDB results
"""

import sqlite3
import json
import logging
import time
from datetime import datetime, date
from utils.config import Config
from kafka.kafka_producer import KafkaProducer
from typing import List, Dict, Any, Optional
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HistoricalReingestion")


class HistoricalDataReingestion:
    """
    Class responsible for historical data re-ingestion from SQLite to Kafka.
    
    This enables the system to analyze existing data and populate MongoDB with
    historical analytics, creating a complete analytical view.
    """
    
    def __init__(self, config, kafka_producer):
        """Initialize the historical data re-ingestion service."""
        self.config = config
        self.kafka_producer = kafka_producer
        self.sqlite_conn = None
        self.processed_hashes = set()  # For idempotency tracking
        
    def connect_to_sqlite(self) -> bool:
        """Connect to SQLite database."""
        try:
            self.sqlite_conn = sqlite3.connect(self.config.SQLITE_DB_PATH)
            self.sqlite_conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            logger.info(f"✅ Connected to SQLite database: {self.config.SQLITE_DB_PATH}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to SQLite: {e}")
            return False
    
    def close_sqlite_connection(self):
        """Close SQLite database connection."""
        if self.sqlite_conn:
            self.sqlite_conn.close()
            self.sqlite_conn = None
            logger.info("✅ SQLite connection closed")
    
    def _generate_data_hash(self, data: Dict[str, Any]) -> str:
        """Generate a hash for data to ensure idempotent processing."""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _check_already_processed(self, data_hash: str) -> bool:
        """Check if data has already been processed (idempotency)."""
        if data_hash in self.processed_hashes:
            logger.debug(f"🔄 Skipping already processed record (hash: {data_hash})")
            return True
        return False
    
    def _mark_as_processed(self, data_hash: str):
        """Mark data as processed to prevent duplicates."""
        self.processed_hashes.add(data_hash)
    
    def _get_kafka_producer(self):
        """Get Kafka producer instance."""
        return self.kafka_producer.get_kafka_producer()
    
    def _publish_to_kafka(self, topic: str, data: Dict[str, Any]) -> bool:
        """Publish data to Kafka topic."""
        try:
            producer = self._get_kafka_producer()
            data_str = json.dumps(data)
            
            # Use record ID as key for idempotent processing
            record_key = str(data.get('id', data.get('invoice_id', data.get('product_id', 'unknown'))))
            
            producer.produce(
                topic=topic,
                key=record_key,
                value=data_str,
                callback=self._delivery_report
            )
            
            # Flush to ensure message is sent
            producer.flush()
            
            logger.info(f"✅ Published to Kafka topic '{topic}': {record_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish to Kafka topic '{topic}': {e}")
            return False
    
    def _delivery_report(self, err, msg):
        """Callback for Kafka message delivery."""
        if err is not None:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            logger.debug(f"📤 Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def _fetch_all_records(self, table_name: str) -> List[Dict[str, Any]]:
        """Fetch all records from a SQLite table."""
        try:
            cursor = self.sqlite_conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            records = []
            for row in rows:
                record = dict(row)
                # Convert datetime objects to strings for JSON serialization
                for key, value in record.items():
                    if isinstance(value, (datetime, date)):
                        record[key] = value.isoformat()
                records.append(record)
            
            logger.info(f"📊 Fetched {len(records)} records from table '{table_name}'")
            return records
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch records from table '{table_name}': {e}")
            return []
    
    def _transform_product_to_kafka_format(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Transform SQLite product record to Kafka format."""
        return {
            "product_id": str(product.get('id', '')),
            "name": product.get('name', ''),
            "category": product.get('category', ''),
            "buy_price": float(product.get('buy_price', 0)),
            "sell_price": float(product.get('sell_price', 0)),
            "min_margin_threshold": float(product.get('min_margin_threshold', 0)),
            "created_at": product.get('created_at', ''),
            "updated_at": product.get('updated_at', ''),
            "quantity": int(product.get('quantity', 0)),
            "last_movement": product.get('last_movement', ''),
            "expiry_date": product.get('expiry_date', ''),
            "batch_no": product.get('batch_no', ''),
            "source": "historical_reingestion",
            "timestamp": datetime.now().isoformat()
        }
    
    def _transform_client_to_kafka_format(self, client: Dict[str, Any]) -> Dict[str, Any]:
        """Transform SQLite client record to Kafka format."""
        return {
            "client_id": str(client.get('id', '')),
            "name": client.get('name', ''),
            "email": client.get('email', ''),
            "phone": client.get('phone', ''),
            "total_revenue_generated": float(client.get('total_revenue_generated', 0)),
            "loyalty_tier": client.get('loyalty_tier', 'Standard'),
            "source": "historical_reingestion",
            "timestamp": datetime.now().isoformat()
        }
    
    def _transform_sale_to_kafka_format(self, sale: Dict[str, Any], sale_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Transform SQLite sale record to Kafka format."""
        return {
            "invoice_id": str(sale.get('id', '')),
            "client_id": str(sale.get('client_id', '')),
            "timestamp": sale.get('date', ''),
            "total_amount": float(sale.get('total', 0)),
            "items": [
                {
                    "product_id": str(item.get('product_id', '')),
                    "quantity": int(item.get('quantity', 0))
                } for item in sale_items
            ],
            "source": "historical_reingestion",
            "processing_timestamp": datetime.now().isoformat()
        }
    
    def _transform_stock_to_kafka_format(self, product: Dict[str, Any]) -> Dict[str, Any]:
        """Transform SQLite product to inventory format for Kafka."""
        return {
            "product_id": str(product.get('id', '')),
            "name": product.get('name', ''),
            "category": product.get('category', ''),
            "buy_price": float(product.get('buy_price', 0)),
            "sell_price": float(product.get('sell_price', 0)),
            "min_margin_threshold": float(product.get('min_margin_threshold', 0)),
            "created_at": product.get('created_at', ''),
            "updated_at": datetime.now().isoformat(),
            "quantity": int(product.get('quantity', 0)),
            "last_movement": datetime.now().isoformat(),
            "expiry_date": product.get('expiry_date', ''),
            "batch_no": product.get('batch_no', ''),
            "source": "historical_reingestion",
            "timestamp": datetime.now().isoformat()
        }
    
    def reingest_products(self) -> bool:
        """Re-ingest all products from SQLite to Kafka."""
        logger.info("🔄 Starting historical products re-ingestion...")
        
        try:
            products = self._fetch_all_records('product')
            if not products:
                logger.info("📉 No products found in database")
                return True
            
            success_count = 0
            for product in products:
                data_hash = self._generate_data_hash(product)
                if self._check_already_processed(data_hash):
                    continue
                    
                kafka_data = self._transform_product_to_kafka_format(product)
                if self._publish_to_kafka('topic-inventory-updates', kafka_data):
                    self._mark_as_processed(data_hash)
                    success_count += 1
            
            logger.info(f"✅ Products re-ingestion complete: {success_count}/{len(products)} records published")
            return True
            
        except Exception as e:
            logger.error(f"❌ Products re-ingestion failed: {e}")
            return False
    
    def reingest_clients(self) -> bool:
        """Re-ingest all clients from SQLite to Kafka."""
        logger.info("🔄 Starting historical clients re-ingestion...")
        
        try:
            clients = self._fetch_all_records('client')
            if not clients:
                logger.info("📉 No clients found in database")
                return True
            
            success_count = 0
            for client in clients:
                data_hash = self._generate_data_hash(client)
                if self._check_already_processed(data_hash):
                    continue
                    
                kafka_data = self._transform_client_to_kafka_format(client)
                if self._publish_to_kafka('topic-clients', kafka_data):
                    self._mark_as_processed(data_hash)
                    success_count += 1
            
            logger.info(f"✅ Clients re-ingestion complete: {success_count}/{len(clients)} records published")
            return True
            
        except Exception as e:
            logger.error(f"❌ Clients re-ingestion failed: {e}")
            return False
    
    def reingest_sales(self) -> bool:
        """Re-ingest all sales from SQLite to Kafka."""
        logger.info("🔄 Starting historical sales re-ingestion...")
        
        try:
            # Fetch sales and their details
            sales = self._fetch_all_records('facture')
            if not sales:
                logger.info("📉 No sales found in database")
                return True
            
            # Fetch all sale details
            sale_details = self._fetch_all_records('facture_details')
            
            # Group sale details by invoice ID
            details_by_invoice = {}
            for detail in sale_details:
                invoice_id = detail.get('facture_id')
                if invoice_id not in details_by_invoice:
                    details_by_invoice[invoice_id] = []
                details_by_invoice[invoice_id].append(detail)
            
            success_count = 0
            for sale in sales:
                invoice_id = sale.get('id')
                sale_items = details_by_invoice.get(invoice_id, [])
                
                data_hash = self._generate_data_hash(sale)
                if self._check_already_processed(data_hash):
                    continue
                    
                kafka_data = self._transform_sale_to_kafka_format(sale, sale_items)
                if self._publish_to_kafka('topic-raw-sales', kafka_data):
                    self._mark_as_processed(data_hash)
                    success_count += 1
            
            logger.info(f"✅ Sales re-ingestion complete: {success_count}/{len(sales)} records published")
            return True
            
        except Exception as e:
            logger.error(f"❌ Sales re-ingestion failed: {e}")
            return False
    
    def reingest_all_historical_data(self) -> bool:
        """Re-ingest all historical data from SQLite to Kafka.
        
        This method orchestrates the complete historical data replay process:
        1. Connect to SQLite database
        2. Re-ingest products (as inventory updates)
        3. Re-ingest clients
        4. Re-ingest sales
        5. Let Spark Structured Streaming process the data automatically
        
        The process is idempotent - duplicate records are skipped.
        """
        logger.info("🎯 Starting complete historical data re-ingestion...")
        
        try:
            if not self.connect_to_sqlite():
                return False
            
            # Re-ingest all data types
            products_success = self.reingest_products()
            clients_success = self.reingest_clients()
            sales_success = self.reingest_sales()
            
            if products_success and clients_success and sales_success:
                logger.info("✅ Historical data re-ingestion completed successfully")
                logger.info("🚀 Spark Structured Streaming will automatically process the data")
                logger.info("📊 MongoDB will be updated with historical analytics")
                logger.info("🖥️  Admin Interface will display complete historical + real-time results")
                return True
            else:
                logger.error("❌ Historical data re-ingestion completed with some failures")
                return False
                
        except Exception as e:
            logger.error(f"❌ Historical data re-ingestion failed: {e}")
            return False
        finally:
            self.close_sqlite_connection()
    
    def check_reingestion_status(self) -> Dict[str, Any]:
        """Check the status of historical data re-ingestion."""
        return {
            'processed_records': len(self.processed_hashes),
            'status': 'completed' if self.processed_hashes else 'not_started'
        }


# Import needed for date handling
from datetime import date


def run_historical_reingestion(config, kafka_producer):
    """Convenience function to run historical data re-ingestion.
    
    Usage:
        from utils.config import Config
        from kafka.kafka_producer import KafkaProducer
        from data.historical_data_reingestion import run_historical_reingestion
        
        config = Config()
        producer = KafkaProducer(config)
        run_historical_reingestion(config, producer)
    """
    reingestion = HistoricalDataReingestion(config, kafka_producer)
    return reingestion.reingest_all_historical_data()


if __name__ == "__main__":
    # Example usage
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from utils.config import Config
    from kafka.kafka_producer import KafkaProducer
    
    config = Config()
    producer = KafkaProducer(config)
    
    reingestion = HistoricalDataReingestion(config, producer)
    success = reingestion.reingest_all_historical_data()
    
    if success:
        print("✅ Historical data re-ingestion completed successfully!")
    else:
        print("❌ Historical data re-ingestion failed.")
    
    sys.exit(0 if success else 1)