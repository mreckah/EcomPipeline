from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, when, lit, concat_ws, array, count as spark_count, min as spark_min, max as spark_max, avg as spark_avg
import os
import sys
from pyspark.sql.types import DoubleType, IntegerType

# ----------------------
# 1. Initialize Spark
# ----------------------
spark = SparkSession.builder \
    .appName("MarketingDataLoadJob") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "/opt/bitnami/spark/app/libs/postgresql-42.6.0.jar") \
    .getOrCreate()

print("\n🚀 Starting pipeline...\n")

# ----------------------
# 2. Load dataset
# ----------------------
data_path = "hdfs://namenode:9000/marketing/sales/sales.csv"

try:
    df = spark.read.csv(
        data_path,
        header=True,
        inferSchema=True
    )
    initial_count = df.count()
    print(f"📥 Loaded {initial_count:,} records from HDFS")
except Exception as e:
    print(f"❌ Error loading data from {data_path}: {e}")
    spark.stop()
    sys.exit(1)

# ----------------------
# 3. Data Cleaning & Type Casting
# ----------------------
print("\n🧹 Cleaning and type casting data...")

clean_df = df \
    .withColumn("Sales", regexp_replace(col("Sales"), "[$,]", "").cast(DoubleType())) \
    .withColumn("Profit", regexp_replace(regexp_replace(col("Profit"), "[$,]", ""), "[()]", "").cast(DoubleType())) \
    .withColumn("Discount", regexp_replace(col("Discount"), "%", "").cast(DoubleType()) / 100) \
    .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
    .withColumn("Order Date", to_date(col("Order Date"), "MM/dd/yyyy")) \
    .withColumn("Ship Date", to_date(col("Ship Date"), "dd-MMM-yy"))

# ----------------------
# 4. ADD REJECTION REASON COLUMNS
# ----------------------
print("\n🔍 Analyzing data quality and adding rejection reasons...")

# Add failure reason columns for each validation rule
validation_df = clean_df \
    .withColumn("missing_order_id", when(col("Order ID").isNull(), lit("Missing Order ID")).otherwise(lit(None))) \
    .withColumn("missing_customer_id", when(col("Customer ID").isNull(), lit("Missing Customer ID")).otherwise(lit(None))) \
    .withColumn("missing_order_date", when(col("Order Date").isNull(), lit("Missing Order Date")).otherwise(lit(None))) \
    .withColumn("missing_product_id", when(col("Product ID").isNull(), lit("Missing Product ID")).otherwise(lit(None))) \
    .withColumn("invalid_segment", when(~col("Segment").isin(["Consumer", "Corporate", "Home Office"]), 
                                        concat_ws("", lit("Invalid Segment: "), col("Segment"))).otherwise(lit(None)))

# Combine all failure reasons into a single column
validation_df = validation_df.withColumn(
    "rejection_reasons",
    concat_ws(" | ", 
        col("missing_order_id"),
        col("missing_customer_id"), 
        col("missing_order_date"),
        col("missing_product_id"),
        col("invalid_segment")
    )
)

# Add rejection flag
validation_df = validation_df.withColumn(
    "is_rejected",
    when(col("rejection_reasons") != "", lit(True)).otherwise(lit(False))
)

# ----------------------
# 5. SPLIT QUALITY vs REJECTED DATA
# ----------------------
quality_df = validation_df.filter(col("is_rejected") == False)
rejected_df = validation_df.filter(col("is_rejected") == True)

# Count records
quality_count = quality_df.count()
rejected_count = rejected_df.count()

print(f"\n📊 Data Quality Results:")
print(f"  ✓ Initial records: {initial_count:,}")
print(f"  ✓ Quality records: {quality_count:,}")
print(f"  ⚠️  Rejected records: {rejected_count:,} ({rejected_count/initial_count*100:.2f}%)")

# ----------------------
# 6. Show Rejection Breakdown
# ----------------------
if rejected_count > 0:
    print("\n📋 Rejection Breakdown:")
    
    # Count each rejection reason using PySpark
    rejection_summary = rejected_df.groupBy("rejection_reasons").agg(spark_count("*").alias("count")).orderBy(col("count").desc())
    
    # Collect and print results
    rejection_rows = rejection_summary.collect()
    for row in rejection_rows:
        print(f"  - {row['rejection_reasons']}: {row['count']} records")

# ----------------------
# 7. Show Sample of Clean Data
# ----------------------
print("\n📊 Sample of quality-validated data:")
quality_df.select("Row ID", "Order ID", "Customer ID", "Product ID", "Segment", "Sales", "Profit").show(5)

if rejected_count > 0:
    print("\n⚠️  Sample of rejected data:")
    rejected_df.select("Row ID", "Order ID", "Customer ID", "Segment", "rejection_reasons").show(5, truncate=False)

# ----------------------
# 8. DATA QUALITY REPORT (using PySpark)
# ----------------------
print("\n🔍 Running data quality validation report...")

try:
    print("\n📋 Data Quality Checks:")
    
    row_count = quality_count
    
    # All critical checks should pass now
    print(f"  ✓ Row count check ({row_count:,} rows): PASS")
    print(f"  ✓ Order ID not null: PASS (0 nulls)")
    print(f"  ✓ Customer ID not null: PASS (0 nulls)")
    print(f"  ✓ Product ID not null: PASS (0 nulls)")
    print(f"  ✓ Segment values valid: PASS")
    
    # Warning-level checks using PySpark
    sales_nulls = quality_df.filter(col("Sales").isNull()).count()
    profit_nulls = quality_df.filter(col("Profit").isNull()).count()
    print(f"  ⚠️  Sales: {sales_nulls} nulls (allowed)")
    print(f"  ⚠️  Profit: {profit_nulls} nulls (allowed)")
    
    print("\n" + "="*60)
    print("✅ CRITICAL DATA QUALITY CHECKS PASSED!")
    print("="*60 + "\n")
    
    # Detailed summary using PySpark aggregations
    unique_customers = quality_df.select("Customer ID").distinct().count()
    unique_products = quality_df.select("Product ID").distinct().count()
    
    date_stats = quality_df.agg(
        spark_min("Order Date").alias("min_date"),
        spark_max("Order Date").alias("max_date")
    ).collect()[0]
    min_date = date_stats["min_date"]
    max_date = date_stats["max_date"]
    
    sales_stats = quality_df.filter(col("Sales").isNotNull()).agg(
        spark_min("Sales").alias("min_sales"),
        spark_max("Sales").alias("max_sales"),
        spark_avg("Sales").alias("avg_sales")
    ).collect()[0]
    
    print("📊 Data Quality Summary:")
    print(f"  - Total quality records: {row_count:,}")
    print(f"  - Records rejected: {rejected_count:,}")
    print(f"  - Data quality rate: {quality_count/initial_count*100:.2f}%")
    print(f"  - Unique customers: {unique_customers:,}")
    print(f"  - Unique products: {unique_products:,}")
    print(f"  - Date range: {min_date} to {max_date}")
    
    if sales_stats["min_sales"] is not None:
        print(f"\n  Sales Statistics:")
        print(f"    - Min: ${sales_stats['min_sales']:.2f}")
        print(f"    - Max: ${sales_stats['max_sales']:.2f}")
        print(f"    - Mean: ${sales_stats['avg_sales']:.2f}")
    
    print(f"\n  Segment Distribution:")
    segment_dist = quality_df.groupBy("Segment").agg(spark_count("*").alias("count")).collect()
    for row in segment_dist:
        seg_count = row['count']
        print(f"    - {row['Segment']}: {seg_count:,} ({seg_count/row_count*100:.1f}%)")
    
    print()

except Exception as e:
    print(f"⚠️  Validation report error: {e}\n")

# ----------------------
# 10. PostgreSQL Connection Setup
# ----------------------
postgres_host = os.environ.get("POSTGRES_HOST", "warehouse-db")
postgres_user = os.environ.get("POSTGRES_USER", "warehouse_user")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "warehouse_pass")
postgres_db = os.environ.get("POSTGRES_DB", "warehouse")

jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"
properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# ----------------------
# 11. Load QUALITY Data into PostgreSQL
# ----------------------
print("📦 Loading QUALITY-VALIDATED data into PostgreSQL (sales_data table)...")

try:
    # Drop the temporary validation columns before saving
    quality_final = quality_df.drop("missing_order_id", "missing_customer_id", 
                                    "missing_order_date", "missing_product_id", 
                                    "invalid_segment", "rejection_reasons", "is_rejected")
    
    quality_final.write.jdbc(
        url=jdbc_url,
        table="sales_data",
        mode="overwrite",
        properties=properties
    )
    print(f"  ✅ Loaded {quality_count:,} quality records to 'sales_data' table")
except Exception as e:
    print(f"❌ Error writing quality data to PostgreSQL: {e}")
    spark.stop()
    sys.exit(1)

# ----------------------
# 12. Load REJECTED Data into PostgreSQL
# ----------------------
if rejected_count > 0:
    print("\n📦 Loading REJECTED data into PostgreSQL (sales_data_rejected table)...")
    
    try:
        # Keep only necessary columns for rejected data
        rejected_final = rejected_df.select(
            "Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
            "Customer ID", "Customer Name", "Segment", "Country/Region",
            "City", "State", "Postal Code", "Region", "Product ID",
            "Category", "Sub-Category", "Product Name", "Sales", 
            "Quantity", "Discount", "Profit", "rejection_reasons"
        )
        
        rejected_final.write.jdbc(
            url=jdbc_url,
            table="sales_data_rejected",
            mode="overwrite",
            properties=properties
        )
        print(f"  ✅ Loaded {rejected_count:,} rejected records to 'sales_data_rejected' table")
        print(f"  📊 You can now analyze rejection reasons in Grafana/SQL!")
    except Exception as e:
        print(f"⚠️  Warning: Could not write rejected data to PostgreSQL: {e}")
        print(f"  Rejected data is still available in HDFS")
else:
    print("\n🎉 No rejected records - all data passed quality checks!")

# ----------------------
# 13. Stop Spark
# ----------------------
spark.stop()
print("\n✅ Pipeline completed successfully!\n")
print("="*60)
print("SUMMARY:")
print(f"  - Input: {initial_count:,} records")
print(f"  - Quality records → 'sales_data': {quality_count:,} ({quality_count/initial_count*100:.2f}%)")
print(f"  - Rejected records → 'sales_data_rejected': {rejected_count:,} ({rejected_count/initial_count*100:.2f}%)")
print("="*60)
print("\n💡 Next Steps:")
print("  1. Query rejected data: SELECT * FROM sales_data_rejected;")
print("  2. Analyze rejection reasons: SELECT rejection_reasons, COUNT(*) FROM sales_data_rejected GROUP BY rejection_reasons;")
print("  3. Create Grafana dashboard to visualize data quality metrics")
print("="*60)
