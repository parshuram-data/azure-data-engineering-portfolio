from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date
import sys

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Bronze-to-Silver") \
    .getOrCreate()

# ---------------------------------------------------------
# Pipeline Parameters
# ---------------------------------------------------------
# Expected arguments:
#   1. Bronze source path
#   2. Silver target path
#
# Example:
#   python bronze_to_silver.py <bronze_path> <silver_path>
# ---------------------------------------------------------

bronze_path = sys.argv[1] if len(sys.argv) > 1 else "data"
silver_path = sys.argv[2] if len(sys.argv) > 2 else "silver"

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
    .dropDuplicates(["product_id"])

# ---------------------------------------------------------
# Orders Transformation
# ---------------------------------------------------------

orders_silver = orders_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("status", upper(trim(col("status")))) \
    .filter(col("quantity") > 0) \
    .dropDuplicates(["order_id"])

# ---------------------------------------------------------
# Payments Transformation
# ---------------------------------------------------------

payments_silver = payments_df \
    .withColumn("payment_date", to_date(col("payment_date"))) \
    .withColumn("payment_method", upper(trim(col("payment_method")))) \
    .withColumn("payment_status", upper(trim(col("payment_status")))) \
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

print("Bronze-to-Silver transformation completed successfully.")

spark.stop()
