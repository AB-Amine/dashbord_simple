#!/usr/bin/env python3
"""
Configuration class for the Real-Time Economic Analytics Platform.
Centralizes all configuration settings for the application.
"""

import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Config:
    """
    Configuration class that holds all application settings.
    """
    
    def __init__(self):
        """Initialize configuration with default values."""
        # Database configuration
        self.DB_NAME = "stock_management"
        self.SQLITE_DB_PATH = "stock_management.db"
        self.MONGO_URI = "mongodb://localhost:27017/"
        
        # Kafka configuration
        self.KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
        self.KAFKA_TOPICS = {
            "inventory": "topic-inventory-updates",
            "clients": "topic-clients", 
            "sales": "topic-raw-sales"
        }
        
        # Spark configuration
        self.SPARK_APP_NAME = "RealTimeAnalytics"
        self.SPARK_JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        self.SPARK_MONGODB_URI = "mongodb://localhost:27017/stock_management"
        
        # File paths
        self.DEFAULT_EXCEL_FILE = "stock_data.xlsx"
        self.ANALYSIS_OUTPUT_DIR = "/tmp/analysis_results"
        
        # Analytics thresholds
        self.REVENUE_THRESHOLD_GOLD = 20000
        self.REVENUE_THRESHOLD_SILVER = 10000
        self.REVENUE_THRESHOLD_PROMO = 10000
        
        # Load environment variables if available
        self._load_environment_variables()
    
    def _load_environment_variables(self):
        """Load configuration from environment variables if available."""
        env_vars = {
            "DB_NAME": "DB_NAME",
            "SQLITE_DB_PATH": "SQLITE_DB_PATH",
            "MONGO_URI": "MONGO_URI",
            "KAFKA_BOOTSTRAP_SERVERS": "KAFKA_BOOTSTRAP_SERVERS",
            "SPARK_APP_NAME": "SPARK_APP_NAME",
            "SPARK_JARS_PACKAGES": "SPARK_JARS_PACKAGES",
            "SPARK_MONGODB_URI": "SPARK_MONGODB_URI",
            "DEFAULT_EXCEL_FILE": "DEFAULT_EXCEL_FILE",
            "ANALYSIS_OUTPUT_DIR": "ANALYSIS_OUTPUT_DIR"
        }
        
        for attr_name, env_name in env_vars.items():
            if env_name in os.environ:
                value = os.environ[env_name]
                setattr(self, attr_name, value)
                logger.info(f"Loaded {attr_name} from environment: {value}")
    
    def get_kafka_topic(self, topic_type: str) -> str:
        """Get Kafka topic name by type."""
        return self.KAFKA_TOPICS.get(topic_type, topic_type)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "DB_NAME": self.DB_NAME,
            "SQLITE_DB_PATH": self.SQLITE_DB_PATH,
            "MONGO_URI": self.MONGO_URI,
            "KAFKA_BOOTSTRAP_SERVERS": self.KAFKA_BOOTSTRAP_SERVERS,
            "KAFKA_TOPICS": self.KAFKA_TOPICS,
            "SPARK_APP_NAME": self.SPARK_APP_NAME,
            "SPARK_JARS_PACKAGES": self.SPARK_JARS_PACKAGES,
            "SPARK_MONGODB_URI": self.SPARK_MONGODB_URI,
            "DEFAULT_EXCEL_FILE": self.DEFAULT_EXCEL_FILE,
            "ANALYSIS_OUTPUT_DIR": self.ANALYSIS_OUTPUT_DIR,
            "REVENUE_THRESHOLD_GOLD": self.REVENUE_THRESHOLD_GOLD,
            "REVENUE_THRESHOLD_SILVER": self.REVENUE_THRESHOLD_SILVER,
            "REVENUE_THRESHOLD_PROMO": self.REVENUE_THRESHOLD_PROMO
        }
    
    def __str__(self) -> str:
        """String representation of configuration."""
        return f"Config({self.to_dict()})"


def get_config() -> Config:
    """Get singleton configuration instance."""
    return Config()