from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Silver-to-Gold") \
    .getOrCreate()

# ---------------------------------------------------------
# ADF / Databricks Parameters
# ---------------------------------------------------------
# ADF passes:
#   source_path = pipeline().parameters.silverFolder
#   target_path = pipeline().parameters.goldFolder
#
# Example:
#   source_path = /mnt/retail/silver
#   target_path = /mnt/retail/gold
# ---------------------------------------------------------

dbutils.widgets.text("source_path", "data/silver")
dbutils.widgets.text("target_path", "data/gold")

source_path = dbutils.widgets.get("source_path")
target_path = dbutils.widgets.get("target_path")

print(f"Silver source path: {source_path}")
print(f"Gold target path: {target_path}")

# ---------------------------------------------------------
# Read Silver Delta Data
# ---------------------------------------------------------

customers = spark.read \
    .format("delta") \
    .load(f"{source_path}/customers")

products = spark.read \
    .format("delta") \
    .load(f"{source_path}/products")

orders = spark.read \
    .format("delta") \
    .load(f"{source_path}/orders")

# ---------------------------------------------------------
# Filter Completed Orders
# ---------------------------------------------------------
# bronze_to_silver standardizes status to uppercase.

completed_orders = orders.filter(
    col("status") == "COMPLETED"
)

# ---------------------------------------------------------
# Calculate Order-Level Revenue
# ---------------------------------------------------------

order_revenue = completed_orders.withColumn(
    "order_value",
    col("quantity") * col("unit_price")
)

# ---------------------------------------------------------
# Gold Dataset 1: Customer Sales
# ---------------------------------------------------------

customer_sales = order_revenue.groupBy(
    "customer_id"
).agg(
    sum("order_value").alias("total_sales"),
    count("order_id").alias("total_orders")
)

gold_customer_sales = customer_sales.join(
    customers,
    on="customer_id",
    how="left"
).select(
    "customer_id",
    "customer_name",
    "city",
    "state",
    "total_orders",
    round("total_sales", 2).alias("total_sales")
)

# ---------------------------------------------------------
# Gold Dataset 2: Product Sales
# ---------------------------------------------------------

product_sales = order_revenue.groupBy(
    "product_id"
).agg(
    sum("quantity").alias("units_sold"),
    sum("order_value").alias("revenue")
)

gold_product_sales = product_sales.join(
    products,
    on="product_id",
    how="left"
).select(
    "product_id",
    "product_name",
    "category",
    "units_sold",
    round("revenue", 2).alias("revenue")
)

# ---------------------------------------------------------
# Write Gold Delta Tables
# ---------------------------------------------------------

gold_customer_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{target_path}/customer_sales")

gold_product_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{target_path}/product_sales")

# ---------------------------------------------------------
# Validation / Output
# ---------------------------------------------------------

print("Customer Sales:")
gold_customer_sales.show()

print("Product Sales:")
gold_product_sales.show()

print("Silver-to-Gold transformation completed successfully.")

spark.stop()
