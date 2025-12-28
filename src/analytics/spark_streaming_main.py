#!/usr/bin/env python3
"""
Standalone Spark Streaming application for real-time analytics.
This runs independently from Streamlit and processes Kafka events continuously.

Usage: python -m src.analytics.spark_streaming_main
"""

import sys
import os
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, desc, when, date_add, date_sub, current_date, avg, rank, current_timestamp, lit, datediff, max as _max, min as _min, mean as _mean
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from pyspark.errors import PySparkException

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SparkStreaming")

class RealTimeAnalyticsStreaming:
    """
    Standalone Spark Streaming application for real-time analytics processing.
    This runs continuously and independently from the Streamlit interface.
    """
    
    def __init__(self):
        """Initialize the streaming application."""
        self.config = Config()
        self.spark = None
        self.streaming_queries = []
        self.shutdown_flag = False
        
    def create_spark_session(self) -> SparkSession:
        """Creates a Spark session with the necessary Kafka and MongoDB packages."""
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName(self.config.SPARK_APP_NAME) \
                .config("spark.jars.packages", self.config.SPARK_JARS_PACKAGES) \
                .config("spark.mongodb.output.uri", self.config.SPARK_MONGODB_URI) \
                .config("spark.mongodb.input.uri", self.config.SPARK_MONGODB_URI) \
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
            logger.info(f"✅ Spark session initialized: {self.spark.sparkContext.appName}")
            logger.info("🚀 Spark streaming started – waiting for Kafka events...")
            return True
        except PySparkException as e:
            logger.error(f"❌ Failed to initialize Spark session: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error initializing Spark session: {e}")
            return False
    
    def stop_streaming_queries(self):
        """Stop all active streaming queries gracefully."""
        try:
            if self.streaming_queries:
                logger.info(f"🛑 Stopping {len(self.streaming_queries)} streaming queries...")
                for query in self.streaming_queries:
                    if query and query.isActive:
                        query.stop()
                        logger.info(f"✅ Stopped streaming query: {query.name if hasattr(query, 'name') else query.id}")
                self.streaming_queries = []
        except PySparkException as e:
            logger.error(f"❌ Error stopping streaming queries: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error stopping streaming queries: {e}")
    
    def shutdown(self):
        """Cleanly shutdown Spark session and streaming queries."""
        try:
            self.shutdown_flag = True
            logger.info("🛑 Initiating graceful shutdown sequence...")
            
            # First stop all streaming queries
            self.stop_streaming_queries()
            
            # Then stop Spark session
            if self.spark:
                logger.info("🔥 Stopping Spark session...")
                self.spark.stop()
                self.spark = None
                logger.info("✅ Spark session stopped successfully")
        except PySparkException as e:
            logger.error(f"❌ Error during Spark shutdown: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error during shutdown: {e}")
        finally:
            logger.info("✅ Shutdown complete")
    
    def process_streams(self):
        """Main streaming processing method that runs continuously."""
        try:
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

            # --- Kafka Streams with Validation ---
            sales_topic = self.config.get_kafka_topic('sales')
            inventory_topic = self.config.get_kafka_topic('inventory')
            client_topic = self.config.get_kafka_topic('clients')
            
            logger.info(f"📡 Connecting to Kafka topics: sales='{sales_topic}', inventory='{inventory_topic}', clients='{client_topic}'")
            logger.info(f"🌐 Kafka bootstrap servers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            
            try:
                sales_stream = self.get_kafka_stream(sales_topic, sales_schema)
                inventory_stream = self.get_kafka_stream(inventory_topic, inventory_schema)
                client_stream = self.get_kafka_stream(client_topic, client_schema)
                logger.info("✅ Successfully connected to all Kafka topics")
            except Exception as e:
                logger.error(f"❌ Failed to connect to Kafka topics: {e}")
                logger.error("🛑 Please ensure Kafka is running and topics exist")
                return False

            # --- Analytics Logic with Event-Driven Conditional MongoDB Writes ---

            def calculate_and_store_winners(df, epoch_id):
                """Product winners analytics - event-driven with conditional write."""
                try:
                    batch_count = df.count()
                    logger.info(f"📊 Product winners batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("📉 No new sales data - keeping previous product winners results")
                        return
                    
                    logger.info("📈 New sales data detected - recalculating product winners with economic analysis")
                    
                    # Try to read from MongoDB first
                    try:
                        product_master = self.spark.read.format("mongo").option("collection", "products").load()
                    except Exception as e:
                        logger.warning(f"🔍 Could not read products from MongoDB, using empty DataFrame: {e}")
                        product_master = self.spark.createDataFrame([], "product_id STRING, name STRING, sell_price FLOAT, buy_price FLOAT, category STRING, min_margin_threshold FLOAT")
                    
                    # Calculate profits and winners with enriched economic data
                    sales_with_profit = df.select("items.product_id", "items.quantity", "timestamp") \
                        .withColumn("product_id", col("product_id")[0]) \
                        .withColumn("quantity", col("quantity")[0]) \
                        .join(product_master, "product_id", "left") \
                        .withColumn("profit", (col("sell_price") - col("buy_price")) * col("quantity")) \
                        .withColumn("revenue", col("sell_price") * col("quantity")) \
                        .withColumn("cost", col("buy_price") * col("quantity")) \
                        .withColumn("profit_margin", (col("sell_price") - col("buy_price")) / col("sell_price") * 100) \
                        .withColumn("roi", (col("sell_price") - col("buy_price")) / col("buy_price") * 100) \
                        .withColumn("sale_date", col("timestamp"))

                    # Get inventory data for stock calculations
                    try:
                        inventory_data = self.spark.read.format("mongo").option("collection", "inventory").load()
                    except Exception as e:
                        logger.warning(f"🔍 Could not read inventory from MongoDB, using empty DataFrame: {e}")
                        inventory_data = self.spark.createDataFrame([], "product_id STRING, quantity INT")
                    
                    # Calculate time period for sales velocity (days between first and last sale)
                    time_period_df = sales_with_profit.groupBy("product_id") \
                        .agg(
                            _min("sale_date").alias("first_sale_date"),
                            _max("sale_date").alias("last_sale_date"),
                            _sum("quantity").alias("total_quantity_sold")
                        ) \
                        .withColumn("time_period_days", 
                            when(col("first_sale_date").isNotNull() & col("last_sale_date").isNotNull(),
                                datediff(col("last_sale_date").cast("date"), col("first_sale_date").cast("date")) + 1)  # +1 to include both days
                            .otherwise(1)  # Default to 1 day if no date range
                        ) \
                        .withColumn("sales_velocity", col("total_quantity_sold") / col("time_period_days"))
                    
                    # Calculate average stock level
                    avg_stock_df = inventory_data.groupBy("product_id") \
                        .agg(_mean("quantity").alias("average_stock_level"))
                    
                    # Join all data for comprehensive analysis
                    comprehensive_sales = sales_with_profit.groupBy("product_id", "name", "category", "sell_price", "buy_price", "min_margin_threshold") \
                        .agg(
                            _sum("profit").alias("total_profit"),
                            _sum("revenue").alias("total_revenue"),
                            _sum("cost").alias("total_cost"),
                            _sum("quantity").alias("total_units_sold"),
                            avg("profit_margin").alias("avg_profit_margin"),
                            avg("roi").alias("avg_roi"),
                            _max("sale_date").alias("last_sale_date")
                        )
                    
                    # Join with time period and inventory data
                    product_winners = comprehensive_sales.join(time_period_df, "product_id", "left") \
                        .join(avg_stock_df, "product_id", "left") \
                        .withColumn("contribution_margin", col("total_profit") / col("total_revenue") * 100) \
                        .withColumn("stock_turnover", 
                            when(col("average_stock_level") > 0, col("total_units_sold") / col("average_stock_level"))
                            .otherwise(0)
                        ) \
                        .withColumn("days_in_stock", 
                            when(col("last_sale_date").isNotNull(),
                                datediff(current_date().cast("date"), col("last_sale_date").cast("date")))
                            .otherwise(0)
                        ) \
                        .withColumn("is_dead_stock", col("days_in_stock") > 30) \
                        .withColumn("sales_velocity", col("sales_velocity")) \
                        .withColumn("performance_score", 
                            (col("total_profit") * 0.5) + 
                            (col("sales_velocity") * 0.3) - 
                            (col("days_in_stock") * 0.2)
                        ) \
                        .withColumn("rank", rank().over(Window.orderBy(desc("performance_score")))) \
                        .orderBy(desc("performance_score")) \
                        .limit(10)

                    # Only write if we have results
                    if product_winners.count() > 0:
                        # Select and rename columns for better readability in admin interface
                        final_winners = product_winners.select(
                            col("product_id").alias("product_id"),
                            col("name").alias("product_name"),
                            col("category").alias("product_category"),
                            col("sell_price").alias("selling_price"),
                            col("buy_price").alias("purchase_price"),
                            col("min_margin_threshold").alias("minimum_margin_threshold"),
                            col("total_profit").alias("total_profit"),
                            col("total_revenue").alias("total_revenue"),
                            col("total_cost").alias("total_cost"),
                            col("total_units_sold").alias("units_sold"),
                            col("avg_profit_margin").alias("average_profit_margin_pct"),
                            col("avg_roi").alias("average_roi_pct"),
                            col("contribution_margin").alias("contribution_margin_pct"),
                            col("sales_velocity").alias("sales_velocity"),
                            col("stock_turnover").alias("stock_turnover"),
                            col("days_in_stock").alias("days_in_stock"),
                            col("is_dead_stock").alias("is_dead_stock"),
                            col("performance_score").alias("performance_score"),
                            col("rank").alias("ranking"),
                            col("last_sale_date").alias("last_sale_date"),
                            current_timestamp().alias("analysis_timestamp"),
                            lit("automated_spark_analysis").alias("analysis_source")
                        )
                        
                        final_winners.write.format("mongo").option("collection", "product_winners").mode("overwrite").save()
                        logger.info("✅ Enriched product winners with advanced analytics written to MongoDB")
                        logger.info(f"📊 Top product: {final_winners.first()['product_name']} with €{final_winners.first()['total_profit']:.2f} profit")
                        logger.info(f"🏆 Performance score: {final_winners.first()['performance_score']:.2f}")
                        logger.info(f"📈 Sales velocity: {final_winners.first()['sales_velocity']:.2f} units/day")
                        logger.info(f"🔄 Stock turnover: {final_winners.first()['stock_turnover']:.2f}")
                    else:
                        logger.info("📉 No product winners to write - keeping previous results")
                        
                except PySparkException as e:
                    logger.error(f"❌ Error in calculate_and_store_winners: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error in calculate_and_store_winners: {e}")

            def detect_and_store_loss_products(df, epoch_id):
                """Loss risk products analytics - event-driven with conditional write."""
                try:
                    batch_count = df.count()
                    logger.info(f"📦 Inventory batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("📉 No new inventory data - keeping previous loss risk products results")
                        return
                    
                    logger.info("📈 New inventory data detected - analyzing loss risk products with economic impact")
                    
                    # Try to read product master data for enrichment
                    try:
                        product_master = self.spark.read.format("mongo").option("collection", "products").load()
                    except Exception as e:
                        logger.warning(f"🔍 Could not read products from MongoDB, using empty DataFrame: {e}")
                        product_master = self.spark.createDataFrame([], "product_id STRING, name STRING, sell_price FLOAT, buy_price FLOAT, category STRING")
                    
                    # Detect products at risk of loss
                    loss_list = df.filter(
                        (col("expiry_date").isNotNull() & (col("expiry_date") < date_add(current_date(), 7))) |
                        (col("last_movement").isNotNull() & (col("last_movement") < date_sub(current_date(), 30)))
                    )
                    
                    # Enrich with product master data and calculate potential loss
                    enriched_loss_list = loss_list.join(product_master, "product_id", "left") \
                        .withColumn("potential_loss", col("quantity") * col("buy_price")) \
                        .withColumn("potential_revenue_loss", col("quantity") * (col("sell_price") - col("buy_price"))) \
                        .withColumn("risk_type", 
                            when(col("expiry_date").isNotNull() & (col("expiry_date") < date_add(current_date(), 7)), "Expiring Soon")
                            .when(col("last_movement").isNotNull() & (col("last_movement") < date_sub(current_date(), 30)), "Dead Stock")
                            .otherwise("Unknown Risk")
                        ) \
                        .withColumn("days_until_expiry", 
                            when(col("expiry_date").isNotNull(), 
                                datediff(col("expiry_date").cast("date"), current_date()))
                            .otherwise(None)
                        ) \
                        .withColumn("days_since_movement", 
                            when(col("last_movement").isNotNull(), 
                                datediff(current_date(), col("last_movement").cast("date")))
                            .otherwise(None)
                        )
                    
                    # Select and rename columns for better readability
                    final_loss_list = enriched_loss_list.select(
                        col("product_id").alias("product_id"),
                        col("name").alias("product_name"),
                        col("category").alias("product_category"),
                        col("quantity").alias("stock_quantity"),
                        col("buy_price").alias("purchase_price"),
                        col("sell_price").alias("selling_price"),
                        col("potential_loss").alias("potential_loss_amount"),
                        col("potential_revenue_loss").alias("potential_profit_loss"),
                        col("risk_type").alias("risk_category"),
                        col("expiry_date").alias("expiry_date"),
                        col("days_until_expiry").alias("days_until_expiry"),
                        col("last_movement").alias("last_movement_date"),
                        col("days_since_movement").alias("days_since_last_movement"),
                        col("batch_no").alias("batch_number"),
                        current_timestamp().alias("analysis_timestamp"),
                        lit("automated_spark_analysis").alias("analysis_source")
                    )
                    
                    # Only write if we have loss risk products
                    if final_loss_list.count() > 0:
                        final_loss_list.write.format("mongo").option("collection", "loss_risk_products").mode("overwrite").save()
                        logger.info(f"✅ Enriched loss risk products with economic analysis written to MongoDB: {final_loss_list.count()} items")
                        logger.info(f"💰 Total potential loss: €{final_loss_list.agg(_sum('potential_loss_amount')).first()[0]:.2f}")
                    else:
                        logger.info("📉 No loss risk products detected - keeping previous results")
                    
                    # Always update inventory (source of truth)
                    df.write.format("mongo").option("collection", "inventory").mode("append").save()
                    logger.info("✅ Inventory data written to MongoDB")
                        
                except PySparkException as e:
                    logger.error(f"❌ Error in detect_and_store_loss_products: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error in detect_and_store_loss_products: {e}")

            def update_client_revenue(df, epoch_id):
                """Client revenue analytics - event-driven with conditional write."""
                try:
                    batch_count = df.count()
                    logger.info(f"👥 Client revenue batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("📉 No new sales data - keeping previous client revenue states")
                        return
                    
                    logger.info("📈 New sales data detected - updating client revenue and promotions")
                    
                    REVENUE_THRESHOLD = self.config.REVENUE_THRESHOLD_PROMO
                    
                    # Try to read existing clients from MongoDB
                    try:
                        client_master = self.spark.read.format("mongo").option("collection", "clients").load()
                    except Exception as e:
                        logger.warning(f"🔍 Could not read clients from MongoDB, creating new collection: {e}")
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
                        logger.info("✅ Client revenue and promotions updated in MongoDB")
                    else:
                        logger.info("📉 No client updates needed - keeping previous states")
                        
                except PySparkException as e:
                    logger.error(f"❌ Error in update_client_revenue: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error in update_client_revenue: {e}")

            def write_clients_to_mongo(df, epoch_id):
                """Client master data - event-driven with conditional write."""
                try:
                    batch_count = df.count()
                    logger.info(f"👥 Client master data batch received - {batch_count} records")
                    
                    if batch_count == 0:
                        logger.info("📉 No new client data - keeping previous client records")
                        return
                    
                    logger.info("📈 New client data detected - updating client master data")
                    df.write.format("mongo").option("collection", "clients").mode("append").save()
                    logger.info("✅ Client master data written to MongoDB")
                except PySparkException as e:
                    logger.error(f"❌ Error in write_clients_to_mongo: {e}")
                except Exception as e:
                    logger.error(f"❌ Unexpected error in write_clients_to_mongo: {e}")

            # --- Start Streaming Queries with Continuous Execution ---
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

            # Validate MongoDB connection before starting streaming
            logger.info(f"🗄️  Connecting to MongoDB: {self.config.SPARK_MONGODB_URI}")
            try:
                # Test MongoDB connection by trying to list collections
                test_df = self.spark.read.format("mongo") \
                    .option("uri", self.config.SPARK_MONGODB_URI) \
                    .option("database", self.config.DB_NAME) \
                    .option("collection", "dummy_test") \
                    .load()
                logger.info("✅ Successfully connected to MongoDB")
            except Exception as e:
                logger.error(f"❌ Failed to connect to MongoDB: {e}")
                logger.error(f"🛑 Please ensure MongoDB is running at: {self.config.SPARK_MONGODB_URI}")
                return False
            
            logger.info("🚀 All Spark streaming queries started - waiting for Kafka events...")
            
            # Keep the streaming queries alive using proper Spark awaitTermination()
            # This is the correct way to run Spark streaming continuously
            logger.info("🔄 Spark streaming queries started - using awaitTermination() for continuous execution...")
            
            # Store the first query for awaitTermination (all queries will run together)
            if self.streaming_queries:
                self.streaming_queries[0].awaitTermination()
            
            return True
            
        except PySparkException as e:
            logger.error(f"❌ Failed to process streams: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error processing streams: {e}")
            return False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("🛑 Received shutdown signal, stopping gracefully...")
    global streaming_app
    if streaming_app:
        streaming_app.shutdown_flag = True  # Set flag to break awaitTermination
        streaming_app.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    # Set up signal handling for graceful shutdown
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize and run the streaming application
    streaming_app = RealTimeAnalyticsStreaming()
    
    try:
        streaming_app.process_streams()
    except KeyboardInterrupt:
        logger.info("\n🛑 Received keyboard interrupt, shutting down...")
        streaming_app.shutdown()
    except Exception as e:
        logger.error(f"❌ Application error: {e}")
        streaming_app.shutdown()
    finally:
        logger.info("✅ Application exit complete")