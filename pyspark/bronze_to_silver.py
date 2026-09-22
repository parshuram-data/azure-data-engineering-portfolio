from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date
from pyspark.sql.types import IntegerType, DoubleType

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Bronze-to-Silver") \
    .getOrCreate()

# ---------------------------------------------------------
# Pipeline Parameters
# ---------------------------------------------------------
# These parameters are passed from Azure Data Factory
# through the Databricks activity.
#
# source_path = Bronze source location
# target_path = Silver target location
#
# Example:
# source_path = data
# target_path = silver
# ---------------------------------------------------------

dbutils.widgets.text("source_path", "data")
dbutils.widgets.text("target_path", "silver")

bronze_path = dbutils.widgets.get("source_path")
silver_path = dbutils.widgets.get("target_path")

print(f"Bronze source path: {bronze_path}")
print(f"Silver target path: {silver_path}")

# ---------------------------------------------------------
# Source Paths
# ---------------------------------------------------------

customers_path = f"{bronze_path}/customers.csv"
products_path = f"{bronze_path}/products.csv"
orders_path = f"{bronze_path}/orders.csv"
payments_path = f"{bronze_path}/payments.csv"

# ---------------------------------------------------------
# Read Bronze Data
# ---------------------------------------------------------

customers_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(customers_path)

products_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(products_path)

orders_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(orders_path)

payments_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(payments_path)

# ---------------------------------------------------------
# Customers Transformation
# ---------------------------------------------------------

customers_silver = customers_df \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state", trim(col("state"))) \
    .withColumn("signup_date", to_date(col("signup_date"))) \
    .dropDuplicates(["customer_id"])

# ---------------------------------------------------------
# Products Transformation
# ---------------------------------------------------------

products_silver = products_df \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", upper(trim(col("category")))) \
    .withColumn("price", col("price").cast(DoubleType())) \
    .filter(col("price") >= 0) \
    .dropDuplicates(["product_id"])

# ---------------------------------------------------------
# Orders Transformation
# ---------------------------------------------------------

orders_silver = orders_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("status", upper(trim(col("status")))) \
    .withColumn("quantity", col("quantity").cast(IntegerType())) \
    .withColumn("unit_price", col("unit_price").cast(DoubleType())) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") >= 0) \
    .dropDuplicates(["order_id"])

# ---------------------------------------------------------
# Payments Transformation
# ---------------------------------------------------------

payments_silver = payments_df \
    .withColumn("payment_date", to_date(col("payment_date"))) \
    .withColumn("payment_method", upper(trim(col("payment_method")))) \
    .withColumn("payment_status", upper(trim(col("payment_status")))) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .filter(col("amount") >= 0) \
    .dropDuplicates(["payment_id"])

# ---------------------------------------------------------
# Write Silver Layer
# ---------------------------------------------------------

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/customers")

products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/products")

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/orders")

payments_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/payments")

# ---------------------------------------------------------
# Completion Message
# ---------------------------------------------------------

print("Bronze-to-Silver transformation completed successfully.")

spark.stop()
