from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, desc, when, date_add, date_sub, current_date, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

def create_spark_session():
    """Creates a Spark session with the necessary Kafka and MongoDB packages."""
    return SparkSession.builder \
        .appName("RealTimeAnalytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/stock_management") \
        .getOrCreate()

def get_kafka_stream(spark, topic_name, schema):
    """Reads a specific Kafka topic into a streaming DataFrame."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def process_streams(spark):
    """Reads from Kafka, processes data, and stores results in MongoDB and Parquet."""
    
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
        StructField("quantity", IntegerType(), True),
        StructField("buy_price", FloatType(), True),
        StructField("sell_price", FloatType(), True),
        StructField("last_movement", StringType(), True),
        StructField("expiry_date", StringType(), True)
    ])

    client_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("total_revenue_generated", FloatType(), True),
        StructField("loyalty_tier", StringType(), True)
    ])

    # --- Kafka Streams ---
    sales_stream = get_kafka_stream(spark, "topic-raw-sales", sales_schema)
    inventory_stream = get_kafka_stream(spark, "topic-inventory-updates", inventory_schema)
    client_stream = get_kafka_stream(spark, "topic-clients", client_schema)

    # --- HDFS Simulation (Cold Storage) ---
    sales_stream.writeStream \
        .format("parquet") \
        .option("path", "/tmp/hdfs_storage/raw_sales") \
        .option("checkpointLocation", "/tmp/checkpoints/sales") \
        .start()

    inventory_stream.writeStream \
        .format("parquet") \
        .option("path", "/tmp/hdfs_storage/inventory_updates") \
        .option("checkpointLocation", "/tmp/checkpoints/inventory") \
        .start()

    client_stream.writeStream \
        .format("parquet") \
        .option("path", "/tmp/hdfs_storage/clients") \
        .option("checkpointLocation", "/tmp/checkpoints/clients") \
        .start()

    # --- Analytics Logic and Hot Storage (MongoDB) ---

    # Function 1: calculate_product_winner()
    def calculate_and_store_winners(df, epoch_id):
        product_master = spark.read.format("mongo").option("collection", "inventory").load()
        
        sales_with_profit = df.select("items.product_id", "items.quantity") \
            .withColumn("product_id", col("product_id")[0]) \
            .withColumn("quantity", col("quantity")[0]) \
            .join(product_master, "product_id") \
            .withColumn("profit", (col("sell_price") - col("buy_price")) * col("quantity"))

        product_winners = sales_with_profit.groupBy("product_id", "name") \
            .agg(_sum("profit").alias("total_profit")) \
            .orderBy(desc("total_profit")) \
            .limit(10)

        product_winners.write.format("mongo").option("collection", "product_winners").mode("overwrite").save()

    # Function 2: detect_loss_products()
    def detect_and_store_loss_products(df, epoch_id):
        loss_list = df.filter(
            (col("expiry_date").isNotNull() & (col("expiry_date") < date_add(current_date(), 7))) |
            (col("last_movement").isNotNull() & (col("last_movement") < date_sub(current_date(), 30)))
        )
        loss_list.write.format("mongo").option("collection", "loss_risk_products").mode("append").save()
        df.write.format("mongo").option("collection", "inventory").mode("append").save()

    # Function 3: apply_client_reduction()
    def update_client_revenue(df, epoch_id):
        REVENUE_THRESHOLD = 10000
        client_master = spark.read.format("mongo").option("collection", "clients").load()

        current_sales_total = df.groupBy("client_id").agg(_sum("total_amount").alias("new_sales"))
        
        updated_clients = client_master.join(current_sales_total, "client_id", "left_outer") \
            .withColumn("new_total_revenue", col("total_revenue_generated") + col("new_sales")) \
            .withColumn("eligible_promo", when(col("new_total_revenue") > REVENUE_THRESHOLD, True).otherwise(False)) \
            .select("client_id", "name", col("new_total_revenue").alias("total_revenue_generated"), "loyalty_tier", "eligible_promo")

        updated_clients.write.format("mongo").option("collection", "clients").mode("overwrite").save()

    def write_clients_to_mongo(df, epoch_id):
        df.write.format("mongo").option("collection", "clients").mode("append").save()

    # --- Write Streams ---
    sales_stream.writeStream.foreachBatch(calculate_and_store_winners).start()
    sales_stream.writeStream.foreachBatch(update_client_revenue).start()
    inventory_stream.writeStream.foreachBatch(detect_and_store_loss_products).start()
    client_stream.writeStream.foreachBatch(write_clients_to_mongo).option("checkpointLocation", "/tmp/checkpoints/clients_mongo").start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    process_streams(spark)