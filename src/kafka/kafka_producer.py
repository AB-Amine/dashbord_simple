#!/usr/bin/env python3
"""
Kafka Producer class for handling data transfer from MongoDB to Kafka topics.
NEW ARCHITECTURE: Replaced SQLite with MongoDB as the primary data source.
"""

import json
import pandas as pd
import numpy as np
import logging
import time
import socket
from typing import List, Dict, Any
from confluent_kafka import Producer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from utils.config import Config
from data.mongodb_accountant_manager import MongoDBAccountantManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Class responsible for producing data to Kafka topics.
    """
    
    def __init__(self, config: Config):
        """Initialize KafkaProducer with configuration."""
        self.config = config
        self.producer = None
        self.mongodb_manager = MongoDBAccountantManager(config)
    
    def acked(self, err, msg):
        """Delivery report callback called on producing a message."""
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            print(f"Message produced to {msg.topic()} [{msg.partition()}]")
    
    def get_kafka_producer(self) -> Producer:
        """Creates and returns a Confluent Kafka producer."""
        if not self.producer:
            conf = {
                'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
                'client.id': socket.gethostname()
            }
            self.producer = Producer(conf)
        return self.producer
    
    def convert_types(self, obj) -> Any:
        """Helper function to convert numpy types to native python types."""
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, list):
            return [self.convert_types(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_types(v) for k, v in obj.items()}
        else:
            return obj
    
    def produce_data(self, topic: str, records: List[Dict[str, Any]]) -> bool:
        """Sends a list of records to a Kafka topic."""
        try:
            if not records:
                logger.warning("No records to produce")
                return True
            
            producer = self.get_kafka_producer()
            
            for record in records:
                try:
                    # Convert numpy/pandas types to native python types for JSON serialization
                    converted_record = self.convert_types(record)
                    
                    producer.produce(
                        topic, 
                        key=None, 
                        value=json.dumps(converted_record).encode('utf-8'), 
                        callback=self.acked
                    )
                    producer.poll(0)
                except (TypeError, ValueError) as e:
                    logger.error(f"Data serialization error: {e}")
                    return False
                except KafkaException as e:
                    logger.error(f"Kafka error sending message: {e}")
                    return False
                except Exception as e:
                    logger.error(f"Unexpected error sending message to Kafka: {e}")
                    return False
            
            # Flush with timeout to avoid hanging
            try:
                remaining = producer.flush(timeout=10)
                if remaining > 0:
                    logger.warning(f"{remaining} messages were not delivered within timeout")
                logger.info(f"Successfully produced {len(records)} messages to topic '{topic}'")
                return True
            except KafkaException as e:
                logger.error(f"Kafka error flushing producer: {e}")
                return False
                
        except KafkaException as e:
            logger.error(f"Kafka error producing data: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error producing data to Kafka: {e}")
            return False
    
    def initialize_topics(self) -> bool:
        """Initialize Kafka topics explicitly using AdminClient."""
        try:
            # Create AdminClient for topic management
            admin_conf = {'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS}
            admin_client = AdminClient(admin_conf)
            
            # Check existing topics
            existing_topics = admin_client.list_topics(timeout=10)
            logger.info(f"Connected to Kafka broker: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"Existing topics: {list(existing_topics.topics.keys())}")
            
            # Prepare topics to create (only those that don't exist)
            topics_to_create = []
            for topic_name in self.config.KAFKA_TOPICS.values():
                if topic_name not in existing_topics.topics:
                    topics_to_create.append(NewTopic(
                        topic_name,
                        num_partitions=3,
                        replication_factor=1
                    ))
            
            # Create missing topics
            if topics_to_create:
                logger.info(f"Creating {len(topics_to_create)} new topics: {[t.topic for t in topics_to_create]}")
                
                # Create topics
                fs = admin_client.create_topics(topics_to_create)
                
                # Wait for topic creation to complete
                for topic, f in fs.items():
                    try:
                        f.result()  # Wait for completion
                        logger.info(f"Topic '{topic}' created successfully")
                    except Exception as e:
                        logger.warning(f"Failed to create topic '{topic}': {e}")
            else:
                logger.info("All required topics already exist")
            
            # Test producer connection
            producer = self.get_kafka_producer()
            metadata = producer.list_topics(timeout=10)
            
            # Verify our topics exist
            for topic_name in self.config.KAFKA_TOPICS.values():
                if topic_name in metadata.topics:
                    logger.info(f"Topic '{topic_name}' is ready")
                else:
                    logger.warning(f"Topic '{topic_name}' not found after creation")
            
            return True
            
        except KafkaException as e:
            logger.error(f"Kafka error initializing topics: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing topics: {e}")
            return False
    
    def transfer_data_from_mongodb(self) -> bool:
        """Transfer data from MongoDB to Kafka topics (NEW ARCHITECTURE)."""
        try:
            # Get data from MongoDB using the new manager
            products = self.mongodb_manager.get_products_for_kafka()
            clients = self.mongodb_manager.get_clients_for_kafka()
            sales = self.mongodb_manager.get_sales_for_kafka()
            inventory = self.mongodb_manager.get_inventory_for_kafka()
            
            # Transfer products to inventory topic
            if products:
                logger.info(f"📦 Transferring {len(products)} products to Kafka")
                if not self.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    products
                ):
                    logger.error("Failed to transfer products to Kafka")
                    return False
            
            # Transfer clients to clients topic
            if clients:
                logger.info(f"👥 Transferring {len(clients)} clients to Kafka")
                if not self.produce_data(
                    self.config.get_kafka_topic('clients'), 
                    clients
                ):
                    logger.error("Failed to transfer clients to Kafka")
                    return False
            
            # Transfer sales to sales topic
            if sales:
                logger.info(f"💰 Transferring {len(sales)} sales to Kafka")
                if not self.produce_data(
                    self.config.get_kafka_topic('sales'), 
                    sales
                ):
                    logger.error("Failed to transfer sales to Kafka")
                    return False
            
            # Transfer inventory to inventory topic
            if inventory:
                logger.info(f"📦 Transferring {len(inventory)} inventory items to Kafka")
                if not self.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    inventory
                ):
                    logger.error("Failed to transfer inventory to Kafka")
                    return False
            
            logger.info("✅ MongoDB to Kafka data transfer completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to transfer data from MongoDB to Kafka: {e}")
            return False
    
    def transfer_data_from_sqlite(self) -> bool:
        """Transfer data from SQLite database to Kafka topics (DEPRECATED)."""
        logger.warning("⚠️  transfer_data_from_sqlite() is deprecated - use transfer_data_from_mongodb()")
        try:
            # Get data from SQLite (old method - kept for backward compatibility)
            data_manager = DataManager(self.config)
            return True
        except Exception as e:
            logger.error(f"❌ Failed in deprecated SQLite transfer: {e}")
            return False
    
    def _transfer_accountant_data(self) -> bool:
        """Transfer data from new accountant tables to Kafka topics (DEPRECATED)."""
        try:
            accountant_manager = AccountantDataManager(self.config)
            
            # Transfer products from accountant tables
            accountant_products = accountant_manager.get_all_products()
            if accountant_products:
                # Convert to match expected schema for inventory topic
                products_for_kafka = []
                for product in accountant_products:
                    products_for_kafka.append({
                        'product_id': product['product_id'],
                        'name': product['name'],
                        'category': product['category'],
                        'buy_price': float(product['buy_price']),
                        'sell_price': float(product['sell_price']),
                        'quantity': 0,  # Default quantity for master data
                        'last_movement': product['created_at'],
                        'expiry_date': None,
                        'batch_no': None
                    })
                
                logger.info(f"Transferring {len(products_for_kafka)} products to Kafka")
                self.produce_data(
                    self.config.get_kafka_topic('inventory'), 
                    products_for_kafka
                )
            
            # Transfer clients from accountant tables
            accountant_clients = accountant_manager.get_all_clients()
            if accountant_clients:
                clients_for_kafka = []
                for client in accountant_clients:
                    clients_for_kafka.append({
                        'client_id': client['client_id'],
                        'name': client['name'],
                        'email': client.get('email'),
                        'phone': client.get('phone'),
                        'total_revenue_generated': float(client['total_revenue_generated']),
                        'loyalty_tier': client['loyalty_tier']
                    })
                
                logger.info(f"Transferring {len(clients_for_kafka)} clients to Kafka")
                self.produce_data(
                    self.config.get_kafka_topic('clients'), 
                    clients_for_kafka
                )
            
            # Transfer sales from accountant tables
            accountant_sales = accountant_manager.get_all_sales()
            if accountant_sales:
                sales_for_kafka = []
                for sale in accountant_sales:
                    # Get sale details
                    sale_details = accountant_manager.get_sale_details(sale['invoice_id'])
                    items = []
                    for detail in sale_details:
                        items.append({
                            'product_id': detail['product_id'],
                            'quantity': int(detail['quantity'])
                        })
                    
                    sales_for_kafka.append({
                        'invoice_id': sale['invoice_id'],
                        'client_id': sale['client_id'],
                        'timestamp': sale['timestamp'],
                        'total_amount': float(sale['total_amount']),
                        'items': items
                    })
                
                logger.info(f"Transferring {len(sales_for_kafka)} sales to Kafka")
                self.produce_data(
                    self.config.get_kafka_topic('sales'), 
                    sales_for_kafka
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to transfer accountant data to Kafka: {e}")
            return False
    
    def shutdown(self):
        """Cleanly shutdown the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer = None
        
        if self.mongodb_manager:
            self.mongodb_manager.shutdown()
            self.mongodb_manager = None