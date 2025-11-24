from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, quarter, month, dayofmonth, weekofyear,
    monotonically_increasing_id
)
import os

# ---------------------------------------------------
# 1. INIT SPARK
# ---------------------------------------------------
spark = SparkSession.builder \
    .appName("BuildGoldStarSchema") \
    .config("spark.jars", "/opt/bitnami/spark/app/libs/postgresql-42.6.0.jar") \
    .getOrCreate()

print("\n🌟 Starting STAR Schema GOLD Layer Build...\n")

# ---------------------------------------------------
# 2. POSTGRES CONFIG
# ---------------------------------------------------
postgres_host = os.getenv("POSTGRES_HOST", "warehouse-db")
postgres_user = os.getenv("POSTGRES_USER", "warehouse_user")
postgres_pass = os.getenv("POSTGRES_PASSWORD", "warehouse_pass")
postgres_db   = os.getenv("POSTGRES_DB", "warehouse")

jdbc_url = f"jdbc:postgresql://{postgres_host}:5432/{postgres_db}"
properties = {
    "driver": "org.postgresql.Driver",
    "user": postgres_user,
    "password": postgres_pass
}

# ---------------------------------------------------
# 3. READ SILVER LAYER (sales_data)
# ---------------------------------------------------
print("📥 Loading SILVER layer: sales_data ...")
sales_df = spark.read.jdbc(url=jdbc_url, table="sales_data", properties=properties)
print(f"Loaded {sales_df.count():,} rows.\n")


# ---------------------------------------------------
# ============= BUILD DIMENSIONS ====================
# ---------------------------------------------------

# -------------------------
# DIM DATE
# -------------------------
print("📅 Building dim_date ...")

dim_date = sales_df \
    .select(col("Order Date").alias("full_date")) \
    .distinct() \
    .filter(col("full_date").isNotNull()) \
    .withColumn("year", year("full_date")) \
    .withColumn("quarter", quarter("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("day", dayofmonth("full_date")) \
    .withColumn("week", weekofyear("full_date")) \
    .withColumn("date_key", monotonically_increasing_id())

dim_date = dim_date.select(
    "date_key", "full_date", "year", "quarter", "month", "day", "week"
)

dim_date.write.jdbc(url=jdbc_url, table="dim_date", mode="overwrite", properties=properties)
print("✅ dim_date created.")


# -------------------------
# DIM CUSTOMER
# -------------------------
print("\n👤 Building dim_customer ...")

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

customer_cols = ["Customer ID", "Customer Name", "Segment"]

dim_customer = sales_df.select(customer_cols).distinct()

dim_customer = dim_customer \
    .withColumn("customer_key", monotonically_increasing_id()) \
    .select("customer_key", col("Customer ID").alias("customer_id"),
            col("Customer Name").alias("customer_name"),
            col("Segment").alias("segment"))

dim_customer.write.jdbc(url=jdbc_url, table="dim_customer", mode="overwrite", properties=properties)
print("✅ dim_customer created.")


# -------------------------
# DIM PRODUCT
# -------------------------
print("\n📦 Building dim_product ...")

dim_product = sales_df.select(
    "Product ID", "Product Name", "Category", "Sub-Category"
).distinct()

dim_product = dim_product \
    .withColumn("product_key", monotonically_increasing_id()) \
    .select(
        "product_key",
        col("Product ID").alias("product_id"),
        col("Product Name").alias("product_name"),
        col("Category").alias("category"),
        col("Sub-Category").alias("sub_category")
    )

dim_product.write.jdbc(url=jdbc_url, table="dim_product", mode="overwrite", properties=properties)
print("✅ dim_product created.")


# -------------------------
# DIM REGION
# -------------------------
print("\n🌎 Building dim_region ...")

dim_region = sales_df.select(
    col("Country/Region").alias("country"),
    "City",
    "State",
    col("Postal Code").alias("postal_code"),
    "Region"
).distinct()

dim_region = dim_region.withColumn("region_key", monotonically_increasing_id())

dim_region = dim_region.select(
    "region_key", "country", "city", "state", "postal_code", "region"
)

dim_region.write.jdbc(url=jdbc_url, table="dim_region", mode="overwrite", properties=properties)
print("✅ dim_region created.")


# ---------------------------------------------------
# ============= BUILD FACT TABLE ====================
# ---------------------------------------------------

print("\n📊 Building fact_sales ...")

# JOIN SILVER → DIMENSIONS
fact_sales = sales_df \
    .join(dim_date, sales_df["Order Date"] == dim_date["full_date"], "left") \
    .join(dim_customer, sales_df["Customer ID"] == dim_customer["customer_id"], "left") \
    .join(dim_product, sales_df["Product ID"] == dim_product["product_id"], "left") \
    .join(dim_region,
          (sales_df["City"] == dim_region["city"]) &
          (sales_df["State"] == dim_region["state"]) &
          (sales_df["Country/Region"] == dim_region["country"]),
          "left") \
    .select(
        monotonically_increasing_id().alias("sales_key"),
        "date_key",
        "customer_key",
        "product_key",
        "region_key",
        col("Sales").alias("sales"),
        col("Profit").alias("profit"),
        col("Quantity").alias("quantity"),
        col("Discount").alias("discount")
    )

fact_sales.write.jdbc(url=jdbc_url, table="fact_sales", mode="overwrite", properties=properties)

print("✅ fact_sales created successfully!")


# ---------------------------------------------------
# DONE
# ---------------------------------------------------
spark.stop()
print("\n🌟 GOLD STAR SCHEMA BUILD FINISHED 🌟")
