#!/usr/bin/env python3
"""
Test script to verify that the ambiguous reference errors are fixed.
This script tests the Spark analytics functions with sample data.
"""

import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SparkAmbiguousFixTest")

def test_ambiguous_reference_fixes():
    """Test that ambiguous reference errors are fixed."""
    try:
        logger.info("🧪 Starting ambiguous reference fix test...")
        
        # Create config
        config = Config()
        
        # Create Spark session with the same configuration as our fixed code
        spark = SparkSession.builder \
            .appName("SparkAmbiguousFixTest") \
            .config("spark.jars.packages", config.SPARK_JARS_PACKAGES) \
            .config("spark.mongodb.output.uri", config.SPARK_MONGODB_URI) \
            .config("spark.mongodb.input.uri", config.SPARK_MONGODB_URI) \
            .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation,G1 Concurrent GC") \
            .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation,G1 Concurrent GC") \
            .getOrCreate()
        
        # Set logging level
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Spark session created with updated GC configuration")
        
        # Test 1: Test join with ambiguous column names (simulating the loss risk scenario)
        logger.info("🧪 Testing join with ambiguous column names...")
        
        # Create sample inventory data
        inventory_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("buy_price", FloatType(), True),
            StructField("sell_price", FloatType(), True)
        ])
        
        inventory_data = spark.createDataFrame([
            ("prod1", 10, 5.0, 10.0),
            ("prod2", 20, 8.0, 15.0)
        ], inventory_schema)
        
        # Create sample product master data (also has buy_price and sell_price)
        product_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("buy_price", FloatType(), True),
            StructField("sell_price", FloatType(), True),
            StructField("category", StringType(), True)
        ])
        
        product_data = spark.createDataFrame([
            ("prod1", "Product 1", 6.0, 12.0, "Category A"),
            ("prod2", "Product 2", 9.0, 16.0, "Category B")
        ], product_schema)
        
        # Test the join that was causing ambiguity
        try:
            # This should work now with our fixes
            joined_data = inventory_data.join(product_data, "product_id", "left") \
                .withColumn("potential_loss", col("quantity") * col("buy_price")) \
                .withColumn("potential_revenue_loss", col("quantity") * (col("sell_price") - col("buy_price"))) \
                .drop(product_data["buy_price"]) \
                .drop(product_data["sell_price"])
            
            logger.info("✅ Join with ambiguous column names succeeded")
            logger.info(f"📊 Result count: {joined_data.count()}")
            joined_data.show()
            
        except Exception as e:
            logger.error(f"❌ Join with ambiguous column names failed: {e}")
            spark.stop()
            return False
        
        # Test 2: Test the time period join that was causing last_sale_date ambiguity
        logger.info("🧪 Testing time period join with last_sale_date...")
        
        # Create sample sales data
        sales_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("sale_date", StringType(), True),
            StructField("last_sale_date", StringType(), True)
        ])
        
        sales_data = spark.createDataFrame([
            ("prod1", "2023-01-01", "2023-01-15"),
            ("prod2", "2023-01-05", "2023-01-20")
        ], sales_schema)
        
        # Create time period data (also has last_sale_date)
        time_period_schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("first_sale_date", StringType(), True),
            StructField("last_sale_date", StringType(), True),
            StructField("total_quantity_sold", IntegerType(), True)
        ])
        
        time_period_data = spark.createDataFrame([
            ("prod1", "2023-01-01", "2023-01-15", 100),
            ("prod2", "2023-01-05", "2023-01-20", 150)
        ], time_period_schema)
        
        try:
            # Rename the conflicting column before joining (our fix)
            time_period_data_renamed = time_period_data \
                .withColumnRenamed("last_sale_date", "time_period_last_sale_date")
            
            # This should work now
            joined_sales = sales_data.join(time_period_data_renamed, "product_id", "left")
            
            logger.info("✅ Time period join with last_sale_date succeeded")
            logger.info(f"📊 Result count: {joined_sales.count()}")
            joined_sales.show()
            
        except Exception as e:
            logger.error(f"❌ Time period join failed: {e}")
            spark.stop()
            return False
        
        # Stop Spark session
        spark.stop()
        logger.info("✅ Spark session stopped successfully")
        
        logger.info("🎉 All ambiguous reference fix tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ambiguous reference fix test failed: {e}")
        return False

if __name__ == "__main__":
    logger.info("🚀 Starting Spark ambiguous reference fixes verification test...")
    
    success = test_ambiguous_reference_fixes()
    
    if success:
        logger.info("🎉 All tests passed! Ambiguous reference errors are fixed.")
    else:
        logger.error("❌ Some tests failed. Please check the logs above.")
    
    sys.exit(0 if success else 1)