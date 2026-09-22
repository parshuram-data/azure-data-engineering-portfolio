from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Silver-to-Gold") \
    .getOrCreate()

# ---------------------------------------------------------
# Pipeline Parameters
# ---------------------------------------------------------
# Azure Data Factory passes:
#   source_path = Silver layer path
#   target_path = Gold layer path
#
# Example:
#   source_path = silver
#   target_path = gold
# ---------------------------------------------------------

dbutils.widgets.text("source_path", "silver")
dbutils.widgets.text("target_path", "gold")

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
# Bronze-to-Silver standardizes order status to uppercase.
#
# Only COMPLETED orders are included in sales calculations.
# CANCELLED orders are excluded from revenue reporting.
# ---------------------------------------------------------

completed_orders = orders.filter(
    col("status") == "COMPLETED"
)

# ---------------------------------------------------------
# Calculate Order-Level Revenue
# ---------------------------------------------------------
# Order value = quantity × unit price
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

print("\n===== GOLD DATASET VALIDATION =====")

customer_sales_count = gold_customer_sales.count()
product_sales_count = gold_product_sales.count()

print(f"Customer Sales records: {customer_sales_count}")
print(f"Product Sales records: {product_sales_count}")

print("\nCustomer Sales:")
gold_customer_sales.show(truncate=False)

print("\nProduct Sales:")
gold_product_sales.show(truncate=False)

print("\nSilver-to-Gold transformation completed successfully.")

# ---------------------------------------------------------
# Stop Spark Session
# ---------------------------------------------------------

spark.stop()
