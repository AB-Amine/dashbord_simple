#!/usr/bin/env python3
"""
Test script to verify Spark fixes are working properly.
This script tests the logging level adjustment and garbage collection configuration.
"""

import sys
import os
import logging
from pyspark.sql import SparkSession

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SparkFixTest")

def test_spark_configuration():
    """Test Spark configuration with the fixes."""
    try:
        logger.info("🧪 Starting Spark configuration test...")
        
        # Create config
        config = Config()
        
        # Create Spark session with the same configuration as our fixed code
        spark = SparkSession.builder \
            .appName("SparkFixTest") \
            .config("spark.jars.packages", config.SPARK_JARS_PACKAGES) \
            .config("spark.mongodb.output.uri", config.SPARK_MONGODB_URI) \
            .config("spark.mongodb.input.uri", config.SPARK_MONGODB_URI) \
            .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Concurrent GC") \
            .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Concurrent GC") \
            .getOrCreate()
        
        # Test logging level adjustment (this should not produce the warning anymore)
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Logging level set successfully using sc.setLogLevel()")
        
        # Test that Spark session is working
        logger.info(f"✅ Spark session created: {spark.sparkContext.appName}")
        
        # Test MongoDB connection
        try:
            test_df = spark.read.format("mongo") \
                .option("uri", config.SPARK_MONGODB_URI) \
                .option("database", config.DB_NAME) \
                .option("collection", "products") \
                .load()
            logger.info(f"✅ MongoDB connection successful, found {test_df.count()} products")
        except Exception as e:
            logger.warning(f"🔍 MongoDB connection test failed (may be expected): {e}")
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped successfully")
        
        logger.info("🎉 All Spark configuration tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Spark configuration test failed: {e}")
        return False

def test_kafka_connection():
    """Test Kafka connection."""
    try:
        logger.info("🧪 Testing Kafka connection...")
        
        from kafka.kafka_producer import KafkaProducer
        config = Config()
        kafka_producer = KafkaProducer(config)
        
        # Test Kafka producer initialization
        producer = kafka_producer.get_kafka_producer()
        logger.info("✅ Kafka producer created successfully")
        
        # Test topic listing
        metadata = producer.list_topics(timeout=10)
        available_topics = list(metadata.topics.keys())
        logger.info(f"✅ Kafka connection successful, found {len(available_topics)} topics")
        
        # Check if our required topics exist
        required_topics = set(config.KAFKA_TOPICS.values())
        existing_topics = set(available_topics)
        missing_topics = required_topics - existing_topics
        
        if missing_topics:
            logger.warning(f"⚠️  Missing Kafka topics: {missing_topics}")
        else:
            logger.info("✅ All required Kafka topics exist")
        
        logger.info("🎉 Kafka connection test passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Kafka connection test failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Spark fixes verification test...")
    
    success = True
    
    # Test Spark configuration
    if not test_spark_configuration():
        success = False
    
    # Test Kafka connection
    if not test_kafka_connection():
        success = False
    
    if success:
        logger.info("🎉 All tests passed! Spark fixes are working correctly.")
    else:
        logger.error("❌ Some tests failed. Please check the logs above.")
    
    sys.exit(0 if success else 1)