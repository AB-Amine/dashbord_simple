#!/usr/bin/env python3
"""
Spark Analytics class for processing data streams and performing analytics.
"""
import threading
import logging
import time
from typing import Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, desc, when, date_add, date_sub, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.errors import PySparkException
from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkAnalytics:
    """
    Class responsible for Spark streaming analytics processing.
    """
    
    def __init__(self, config: Config):
        """Initialize SparkAnalytics with configuration."""
        self.config = config
        self.spark = None
        self.streaming_queries = []
        self.shutdown_flag = False
        self.spark_thread = None
    
    def create_spark_session(self) -> SparkSession:
        """Creates a Spark session with the necessary Kafka and MongoDB packages."""
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName(self.config.SPARK_APP_NAME) \
                .config("spark.jars.packages", self.config.SPARK_JARS_PACKAGES) \
                .config("spark.mongodb.output.uri", self.config.SPARK_MONGODB_URI) \
                .getOrCreate()
        return self.spark
    
    def get_kafka_stream(self, topic_name: str, schema: StructType):
        """Reads a specific Kafka topic into a streaming DataFrame."""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load() \
            .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    def initialize_spark(self) -> bool:
        """Initialize Spark session."""
        try:
            self.spark = self.create_spark_session()
            # Set logging level using the recommended approach
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Spark session initialized: {self.spark.sparkContext.appName}")
            return True
        except PySparkException as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error initializing Spark session: {e}")
            return False
    
    def process_streams(self) -> bool:
        """Reads from Kafka, processes data, and stores results in MongoDB and Parquet."""
        try:
            if not self.spark:
                if not self.initialize_spark():
                    return False
            
            # Clear any existing streaming queries
            self.stop_streaming_queries()
            
            # --- Schemas Definition ---
            sales_schema = StructType([
                StructField("invoice_id", StringType(), True),
                StructField("client_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("total_amount", FloatType(), True),
                StructField("items", ArrayType(StructType([
                    StructField("product_id", StringType(), True),
                    StructField("quantity", IntegerType(), True)
                ])), True)
            ])

            inventory_schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("buy_price", FloatType(), True),
                StructField("sell_price", FloatType(), True),
                StructField("min_margin_threshold", FloatType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("last_movement", StringType(), True),
                StructField("expiry_date", StringType(), True),
                StructField("batch_no", StringType(), True)
            ])

            client_schema = StructType([
                StructField("client_id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("total_revenue_generated", FloatType(), True),
                StructField("loyalty_tier", StringType(), True)
            ])

            # --- Kafka Streams ---
            sales_stream = self.get_kafka_stream(
                self.config.get_kafka_topic('sales'), 
                sales_schema
            )
            inventory_stream = self.get_kafka_stream(
                self.config.get_kafka_topic('inventory'), 
                inventory_schema
            )
            client_stream = self.get_kafka_stream(
                self.config.get_kafka_topic('clients'), 
                client_schema
            )

            # --- Analytics Logic with Event-Driven Conditional MongoDB Writes ---

            # Function 1: calculate_product_winner() - Event-driven with conditional write
            def calculate_and_store_winners(df, epoch_id):
                try:
                    batch_count = df.count()
                    logger.info(f"Product winners batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("No new sales data - keeping previous product winners results")
                        return
                    
                    logger.info("New sales data detected - recalculating product winners")
                    
                    # Try to read from MongoDB first, if not available use empty DataFrame
                    try:
                        product_master = self.spark.read.format("mongo").option("collection", "products").load()
                    except Exception as e:
                        logger.warning(f"Could not read products from MongoDB, using empty DataFrame: {e}")
                        product_master = self.spark.createDataFrame([], "product_id STRING, name STRING, sell_price FLOAT, buy_price FLOAT")
                    
                    sales_with_profit = df.select("items.product_id", "items.quantity") \
                        .withColumn("product_id", col("product_id")[0]) \
                        .withColumn("quantity", col("quantity")[0]) \
                        .join(product_master, "product_id", "left") \
                        .withColumn("profit", (col("sell_price") - col("buy_price")) * col("quantity"))

                    product_winners = sales_with_profit.groupBy("product_id", "name") \
                        .agg(_sum("profit").alias("total_profit")) \
                        .orderBy(desc("total_profit")) \
                        .limit(10)

                    # Only write if we have results
                    if product_winners.count() > 0:
                        product_winners.write.format("mongo").option("collection", "product_winners").mode("overwrite").save()
                        logger.info("Product winners successfully written to MongoDB")
                    else:
                        logger.info("No product winners to write - keeping previous results")
                        
                except PySparkException as e:
                    logger.error(f"Error in calculate_and_store_winners: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in calculate_and_store_winners: {e}")

            # Function 2: detect_loss_products() - Event-driven with conditional write
            def detect_and_store_loss_products(df, epoch_id):
                try:
                    batch_count = df.count()
                    logger.info(f"Inventory batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("No new inventory data - keeping previous loss risk products results")
                        return
                    
                    logger.info("New inventory data detected - analyzing loss risk products")
                    
                    loss_list = df.filter(
                        (col("expiry_date").isNotNull() & (col("expiry_date") < date_add(current_date(), 7))) |
                        (col("last_movement").isNotNull() & (col("last_movement") < date_sub(current_date(), 30)))
                    )
                    
                    # Only write if we have loss risk products
                    if loss_list.count() > 0:
                        loss_list.write.format("mongo").option("collection", "loss_risk_products").mode("append").save()
                        logger.info(f"Loss risk products written to MongoDB: {loss_list.count()} items")
                    else:
                        logger.info("No loss risk products detected - keeping previous results")
                    
                    # Always update inventory (this is the source of truth)
                    df.write.format("mongo").option("collection", "inventory").mode("append").save()
                    logger.info("Inventory data written to MongoDB")
                        
                except PySparkException as e:
                    logger.error(f"Error in detect_and_store_loss_products: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in detect_and_store_loss_products: {e}")

            # Function 3: update_client_revenue() - Event-driven with conditional write
            def update_client_revenue(df, epoch_id):
                try:
                    batch_count = df.count()
                    logger.info(f"Client revenue batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("No new sales data - keeping previous client revenue states")
                        return
                    
                    logger.info("New sales data detected - updating client revenue and promotions")
                    
                    REVENUE_THRESHOLD = self.config.REVENUE_THRESHOLD_PROMO
                    
                    # Try to read existing clients from MongoDB
                    try:
                        client_master = self.spark.read.format("mongo").option("collection", "clients").load()
                    except Exception as e:
                        logger.warning(f"Could not read clients from MongoDB, creating new collection: {e}")
                        client_master = self.spark.createDataFrame([], "client_id STRING, name STRING, total_revenue_generated FLOAT, loyalty_tier STRING")

                    current_sales_total = df.groupBy("client_id").agg(_sum("total_amount").alias("new_sales"))
                    
                    # Only update if we have sales data
                    if current_sales_total.count() > 0:
                        updated_clients = client_master.join(current_sales_total, "client_id", "left_outer") \
                            .withColumn("new_total_revenue", 
                                when(col("total_revenue_generated").isNull(), col("new_sales"))
                                .otherwise(col("total_revenue_generated") + col("new_sales"))
                            ) \
                            .withColumn("eligible_promo", when(col("new_total_revenue") > REVENUE_THRESHOLD, True).otherwise(False)) \
                            .select("client_id", "name", col("new_total_revenue").alias("total_revenue_generated"), "loyalty_tier", "eligible_promo")

                        updated_clients.write.format("mongo").option("collection", "clients").mode("overwrite").save()
                        logger.info("Client revenue and promotions updated in MongoDB")
                    else:
                        logger.info("No client updates needed - keeping previous states")
                        
                except PySparkException as e:
                    logger.error(f"Error in update_client_revenue: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in update_client_revenue: {e}")

            def write_clients_to_mongo(df, epoch_id):
                try:
                    batch_count = df.count()
                    logger.info(f"Client master data batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("No new client data - keeping previous client records")
                        return
                    
                    logger.info("New client data detected - updating client master data")
                    df.write.format("mongo").option("collection", "clients").mode("append").save()
                    logger.info("Client master data written to MongoDB")
                except PySparkException as e:
                    logger.error(f"Error in write_clients_to_mongo: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error in write_clients_to_mongo: {e}")

            # --- Write Streams with Continuous Processing ---
            # Start streaming queries with proper checkpointing and continuous execution
            sales_winners_query = sales_stream.writeStream \
                .foreachBatch(calculate_and_store_winners) \
                .outputMode("update") \
                .option("checkpointLocation", "/tmp/checkpoints/sales_winners") \
                .trigger(processingTime="10 seconds") \
                .start()

            sales_revenue_query = sales_stream.writeStream \
                .foreachBatch(update_client_revenue) \
                .outputMode("update") \
                .option("checkpointLocation", "/tmp/checkpoints/sales_revenue") \
                .trigger(processingTime="10 seconds") \
                .start()

            inventory_loss_query = inventory_stream.writeStream \
                .foreachBatch(detect_and_store_loss_products) \
                .outputMode("update") \
                .option("checkpointLocation", "/tmp/checkpoints/inventory_loss") \
                .trigger(processingTime="10 seconds") \
                .start()

            clients_mongo_query = client_stream.writeStream \
                .foreachBatch(write_clients_to_mongo) \
                .outputMode("update") \
                .option("checkpointLocation", "/tmp/checkpoints/clients_mongo") \
                .trigger(processingTime="10 seconds") \
                .start()

            # Store query references for graceful shutdown
            self.streaming_queries = [
                sales_winners_query,
                sales_revenue_query,
                inventory_loss_query,
                clients_mongo_query
            ]

            logger.info("Spark streaming queries started in background - waiting for Kafka events...")
            
            # Keep the streaming queries alive indefinitely
            while not self.shutdown_flag and any(query.isActive for query in self.streaming_queries):
                time.sleep(5)
                logger.info("Spark streaming queries running continuously...")

            return True
            
        except PySparkException as e:
            logger.error(f"Failed to process streams: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing streams: {e}")
            return False
    
    def stop_streaming_queries(self):
        """Stop all active streaming queries gracefully."""
        try:
            if self.streaming_queries:
                logger.info(f"Stopping {len(self.streaming_queries)} streaming queries...")
                for query in self.streaming_queries:
                    if query and query.isActive:
                        query.stop()
                        logger.info(f"Stopped streaming query: {query.name if hasattr(query, 'name') else query.id}")
                self.streaming_queries = []
        except PySparkException as e:
            logger.error(f"Error stopping streaming queries: {e}")
        except Exception as e:
            logger.error(f"Unexpected error stopping streaming queries: {e}")

    def run_analytics(self) -> bool:
        """Run batch analytics processing (non-streaming version for admin interface)."""
        try:
            logger.info("🚀 Starting batch analytics processing...")
            
            if not self.spark:
                if not self.initialize_spark():
                    logger.error("❌ Failed to initialize Spark for batch processing")
                    return False
            
            # Run the streaming processing which includes batch analytics
            success = self.process_streams()
            
            if success:
                logger.info("✅ Batch analytics completed successfully")
                return True
            else:
                logger.error("❌ Batch analytics failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Exception during batch analytics: {e}")
            return False

    def shutdown(self):
        """Cleanly shutdown Spark session and streaming queries."""
        try:
            # First stop all streaming queries
            self.stop_streaming_queries()
            
            # Then stop Spark session
            if self.spark:
                logger.info("Stopping Spark session...")
                self.spark.stop()
                self.spark = None
                logger.info("Spark session stopped successfully")
        except PySparkException as e:
            logger.error(f"Error during Spark shutdown: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during shutdown: {e}")
        finally:
            self.shutdown_flag = True
            if self.spark_thread and self.spark_thread.is_alive():
                self.spark_thread.join(timeout=5)
                if self.spark_thread.is_alive():
                    logger.warning("Spark thread did not terminate gracefully")