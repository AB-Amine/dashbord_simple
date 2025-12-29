#!/usr/bin/env python3
"""
Fix script for Spark SQL errors in the streaming analytics.
This script identifies and fixes the three main errors:
1. AMBIGUOUS_REFERENCE: buy_price is ambiguous
2. UNRESOLVED_COLUMN: product_id cannot be resolved  
3. UNRESOLVED_USING_COLUMN_FOR_JOIN: client_id cannot be resolved
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def analyze_spark_errors():
    """Analyze the Spark SQL errors and identify fixes."""
    print("🔍 Analyzing Spark SQL Errors...")
    print("=" * 60)
    
    errors = [
        {
            "error": "AMBIGUOUS_REFERENCE",
            "message": "Reference `buy_price` is ambiguous, could be: [`buy_price`, `buy_price`].",
            "location": "detect_and_store_loss_products",
            "cause": "Joining inventory with products creates duplicate buy_price columns",
            "fix": "Use qualified column names or drop duplicate columns after join"
        },
        {
            "error": "UNRESOLVED_COLUMN", 
            "message": "A column or function parameter with name `product_id` cannot be resolved.",
            "location": "calculate_and_store_winners",
            "cause": "Missing product_id in the aggregation result or incorrect column reference",
            "fix": "Ensure product_id is included in groupBy and properly referenced"
        },
        {
            "error": "UNRESOLVED_USING_COLUMN_FOR_JOIN",
            "message": "USING column `client_id` cannot be resolved on the left side of the join.",
            "location": "update_client_revenue",
            "cause": "Left DataFrame in join doesn't have client_id column",
            "fix": "Use explicit join condition instead of USING clause or ensure column exists"
        }
    ]
    
    print("📋 Identified Spark SQL Errors:")
    for i, error in enumerate(errors, 1):
        print(f"\n{i}. {error['error']}: {error['message']}")
        print(f"   Location: {error['location']}")
        print(f"   Cause: {error['cause']}")
        print(f"   Fix: {error['fix']}")
    
    return errors

def create_fixes():
    """Create the specific fixes for each error."""
    print("\n🔧 Creating Fixes...")
    
    fixes = {
        "calculate_and_store_winners": """
        # FIX 1: Ensure product_id is properly included in groupBy
        comprehensive_sales = sales_with_profit.groupBy(
            "product_id", "name", "category", "sell_price", "buy_price", "min_margin_threshold"
        )
        
        # FIX 2: Use explicit column references to avoid ambiguity
        product_winners = comprehensive_sales.join(time_period_df_renamed, "product_id", "left") \
            .join(avg_stock_df, "product_id", "left")
        """,
        
        "detect_and_store_loss_products": """
        # FIX 1: Use qualified column names to avoid ambiguity
        enriched_loss_list = loss_list.join(product_master, "product_id", "left") \
            .withColumn("potential_loss", col("loss_list.quantity") * col("product_master.buy_price")) \
            .withColumn("potential_revenue_loss", col("loss_list.quantity") * (col("product_master.sell_price") - col("product_master.buy_price")))
        """,
        
        "update_client_revenue": """
        # FIX 1: Use explicit join condition instead of relying on column names
        updated_clients = client_master.join(current_sales_total, client_master["client_id"] == current_sales_total["client_id"], "left_outer")
        """
    }
    
    for method, fix in fixes.items():
        print(f"\n📝 Fix for {method}:")
        print("```python")
        print(fix)
        print("```")
    
    return fixes

def apply_fixes_to_file():
    """Apply the fixes to the actual Spark streaming file."""
    print("\n🛠️ Applying Fixes to Spark Streaming File...")
    
    try:
        # Read the current file
        with open('src/analytics/spark_streaming_main.py', 'r') as f:
            content = f.read()
        
        # Fix 1: Calculate and store winners - ensure product_id in groupBy
        if 'comprehensive_sales = sales_with_profit.groupBy("product_id"' in content:
            print("✅ Fix 1: product_id already in groupBy")
        else:
            print("⚠️  Fix 1: Need to add product_id to groupBy")
        
        # Fix 2: Detect loss products - use qualified column names
        if 'col("loss_list.quantity")' in content or 'col("product_master.buy_price")' in content:
            print("✅ Fix 2: Qualified column names already used")
        else:
            print("⚠️  Fix 2: Need to add qualified column names")
        
        # Fix 3: Update client revenue - use explicit join condition
        if 'client_master["client_id"] == current_sales_total["client_id"]' in content:
            print("✅ Fix 3: Explicit join condition already used")
        else:
            print("⚠️  Fix 3: Need to fix join condition")
        
        print("✅ Analysis complete. Manual fixes may be needed.")
        return True
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        return False

def create_fixed_versions():
    """Create fixed versions of the problematic methods."""
    print("\n📄 Creating Fixed Method Versions...")
    
    fixed_methods = {
        "calculate_and_store_winners_fixed": """
    def calculate_and_store_winners_fixed(df, epoch_id):
        """Fixed version of product winners analytics."""
        try:
            batch_count = df.count()
            if batch_count == 0:
                return
            
            # Read product master data
            try:
                product_master = self.spark.read.format("mongo").option("collection", "products").load()
            except Exception as e:
                product_master = self.spark.createDataFrame([], "product_id STRING, name STRING, sell_price FLOAT, buy_price FLOAT, category STRING, min_margin_threshold FLOAT")
            
            # Calculate sales with profit - FIX: Ensure proper column extraction
            sales_with_profit = df.select("items.product_id", "items.quantity", "timestamp") \
                .withColumn("product_id", col("product_id")[0]) \
                .withColumn("quantity", col("quantity")[0]) \
                .join(product_master, "product_id", "left") \
                .withColumn("profit", (col("sell_price") - col("buy_price")) * col("quantity"))
            
            # FIX: Include product_id in all groupBy operations
            comprehensive_sales = sales_with_profit.groupBy(
                "product_id", "name", "category", "sell_price", "buy_price", "min_margin_threshold"
            ).agg(
                _sum("profit").alias("total_profit"),
                _sum("revenue").alias("total_revenue"),
                _sum("quantity").alias("total_units_sold")
            )
            
            # FIX: Use explicit column references
            product_winners = comprehensive_sales.orderBy(desc("total_profit"))
            
            if product_winners.count() > 0:
                final_winners = product_winners.select(
                    col("product_id").alias("product_id"),
                    col("name").alias("product_name"),
                    col("category").alias("category"),
                    col("total_profit").alias("total_profit"),
                    col("total_revenue").alias("total_revenue"),
                    col("total_units_sold").alias("total_units_sold")
                )
                
                final_winners.write.format("mongo").option("collection", "product_winners").mode("overwrite").save()
                logger.info("✅ Product winners updated in MongoDB")
            
        except Exception as e:
            logger.error(f"❌ Error in calculate_and_store_winners_fixed: {e}")
        """,
        
        "detect_and_store_loss_products_fixed": """
    def detect_and_store_loss_products_fixed(df, epoch_id):
        """Fixed version of loss risk products analytics."""
        try:
            batch_count = df.count()
            if batch_count == 0:
                return
            
            # Read product master data
            try:
                product_master = self.spark.read.format("mongo").option("collection", "products").load()
            except Exception as e:
                product_master = self.spark.createDataFrame([], "product_id STRING, name STRING, sell_price FLOAT, buy_price FLOAT, category STRING")
            
            # Detect products at risk
            loss_list = df.filter(
                (col("expiry_date").isNotNull() & (col("expiry_date") < date_add(current_date(), 7))) |
                (col("last_movement").isNotNull() & (col("last_movement") < date_sub(current_date(), 30)))
            )
            
            # FIX: Use qualified column names to avoid ambiguity
            enriched_loss_list = loss_list.join(product_master, "product_id", "left") \
                .withColumn("potential_loss", col("loss_list.quantity") * col("product_master.buy_price")) \
                .withColumn("potential_revenue_loss", col("loss_list.quantity") * (col("product_master.sell_price") - col("product_master.buy_price")))
            
            if enriched_loss_list.count() > 0:
                final_loss_list = enriched_loss_list.select(
                    col("product_id").alias("product_id"),
                    col("name").alias("product_name"),
                    col("category").alias("category"),
                    col("potential_loss").alias("potential_loss"),
                    col("potential_revenue_loss").alias("potential_revenue_loss")
                )
                
                final_loss_list.write.format("mongo").option("collection", "loss_risk_products").mode("overwrite").save()
                logger.info("✅ Loss risk products updated in MongoDB")
            
        except Exception as e:
            logger.error(f"❌ Error in detect_and_store_loss_products_fixed: {e}")
        """,
        
        "update_client_revenue_fixed": """
    def update_client_revenue_fixed(df, epoch_id):
        """Fixed version of client revenue analytics."""
        try:
            batch_count = df.count()
            if batch_count == 0:
                return
            
            # Read existing clients
            try:
                client_master = self.spark.read.format("mongo").option("collection", "clients").load()
            except Exception as e:
                client_master = self.spark.createDataFrame([], "client_id STRING, name STRING, total_revenue_generated FLOAT, loyalty_tier STRING")
            
            current_sales_total = df.groupBy("client_id").agg(_sum("total_amount").alias("new_sales"))
            
            # FIX: Use explicit join condition
            if current_sales_total.count() > 0:
                updated_clients = client_master.join(current_sales_total, client_master["client_id"] == current_sales_total["client_id"], "left_outer") \
                    .withColumn("new_total_revenue", 
                        when(col("total_revenue_generated").isNull(), col("new_sales"))
                        .otherwise(col("total_revenue_generated") + col("new_sales"))
                    ) \
                    .select("client_id", "name", col("new_total_revenue").alias("total_revenue_generated"), "loyalty_tier")
                
                updated_clients.write.format("mongo").option("collection", "clients").mode("overwrite").save()
                logger.info("✅ Client revenue updated in MongoDB")
            
        except Exception as e:
            logger.error(f"❌ Error in update_client_revenue_fixed: {e}")
        """
    }
    
    return fixed_methods

def main():
    """Run the Spark error analysis and fix process."""
    print("🚀 Spark SQL Error Analysis and Fix Tool")
    print("=" * 60)
    
    # Step 1: Analyze errors
    errors = analyze_spark_errors()
    
    # Step 2: Create fixes
    fixes = create_fixes()
    
    # Step 3: Apply fixes
    apply_fixes_to_file()
    
    # Step 4: Create fixed methods
    fixed_methods = create_fixed_versions()
    
    print("\n" + "=" * 60)
    print("📋 SUMMARY:")
    print(f"  Errors Analyzed: {len(errors)}")
    print(f"  Fixes Created: {len(fixes)}")
    print(f"  Fixed Methods: {len(fixed_methods)}")
    
    print("\n🎯 RECOMMENDATION:")
    print("1. Apply the fixes to src/analytics/spark_streaming_main.py")
    print("2. Test the fixed methods individually")
    print("3. Monitor Spark logs for any remaining errors")
    print("4. The fixes address:")
    print("   - Column ambiguity in joins")
    print("   - Missing columns in aggregations")
    print("   - Explicit join conditions")

if __name__ == "__main__":
    main()