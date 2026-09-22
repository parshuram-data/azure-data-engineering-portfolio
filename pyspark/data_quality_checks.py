from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Data-Quality-Checks") \
    .getOrCreate()

# ---------------------------------------------------------
# Pipeline Parameters
# ---------------------------------------------------------
# Azure Data Factory passes the Silver layer path
# through the Databricks notebook activity.
#
# Example:
# input_path = silver
# ---------------------------------------------------------

dbutils.widgets.text("input_path", "silver")

silver_path = dbutils.widgets.get("input_path")

print(f"Silver input path: {silver_path}")

# ---------------------------------------------------------
# Read Silver Delta Tables
# ---------------------------------------------------------

customers_df = spark.read \
    .format("delta") \
    .load(f"{silver_path}/customers")

products_df = spark.read \
    .format("delta") \
    .load(f"{silver_path}/products")

orders_df = spark.read \
    .format("delta") \
    .load(f"{silver_path}/orders")

payments_df = spark.read \
    .format("delta") \
    .load(f"{silver_path}/payments")

print("===== DATA QUALITY CHECKS =====")

# ---------------------------------------------------------
# Customers Data Quality
# ---------------------------------------------------------

print("\n===== CUSTOMERS =====")

customer_record_count = customers_df.count()

customer_duplicate_count = customers_df \
    .groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

customer_null_id = customers_df \
    .filter(col("customer_id").isNull()) \
    .count()

customer_null_name = customers_df \
    .filter(col("customer_name").isNull()) \
    .count()

customer_null_signup_date = customers_df \
    .filter(col("signup_date").isNull()) \
    .count()

print(f"Total customer records: {customer_record_count}")
print(f"Duplicate customer IDs: {customer_duplicate_count}")
print(f"NULL customer IDs: {customer_null_id}")
print(f"NULL customer names: {customer_null_name}")
print(f"NULL signup dates: {customer_null_signup_date}")

# ---------------------------------------------------------
# Products Data Quality
# ---------------------------------------------------------

print("\n===== PRODUCTS =====")

product_record_count = products_df.count()

product_duplicate_count = products_df \
    .groupBy("product_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

product_null_id = products_df \
    .filter(col("product_id").isNull()) \
    .count()

product_null_name = products_df \
    .filter(col("product_name").isNull()) \
    .count()

product_null_price = products_df \
    .filter(col("price").isNull()) \
    .count()

product_negative_price = products_df \
    .filter(col("price") < 0) \
    .count()

print(f"Total product records: {product_record_count}")
print(f"Duplicate product IDs: {product_duplicate_count}")
print(f"NULL product IDs: {product_null_id}")
print(f"NULL product names: {product_null_name}")
print(f"NULL product prices: {product_null_price}")
print(f"Negative product prices: {product_negative_price}")

# ---------------------------------------------------------
# Orders Data Quality
# ---------------------------------------------------------

print("\n===== ORDERS =====")

order_record_count = orders_df.count()

order_duplicate_count = orders_df \
    .groupBy("order_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

order_null_id = orders_df \
    .filter(col("order_id").isNull()) \
    .count()

order_null_customer_id = orders_df \
    .filter(col("customer_id").isNull()) \
    .count()

order_null_product_id = orders_df \
    .filter(col("product_id").isNull()) \
    .count()

order_null_date = orders_df \
    .filter(col("order_date").isNull()) \
    .count()

order_invalid_quantity = orders_df \
    .filter(col("quantity") <= 0) \
    .count()

order_invalid_price = orders_df \
    .filter(col("unit_price") < 0) \
    .count()

print(f"Total order records: {order_record_count}")
print(f"Duplicate order IDs: {order_duplicate_count}")
print(f"NULL order IDs: {order_null_id}")
print(f"NULL customer IDs: {order_null_customer_id}")
print(f"NULL product IDs: {order_null_product_id}")
print(f"NULL order dates: {order_null_date}")
print(f"Invalid quantities: {order_invalid_quantity}")
print(f"Negative unit prices: {order_invalid_price}")

# ---------------------------------------------------------
# Payments Data Quality
# ---------------------------------------------------------

print("\n===== PAYMENTS =====")

payment_record_count = payments_df.count()

payment_duplicate_count = payments_df \
    .groupBy("payment_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

payment_null_id = payments_df \
    .filter(col("payment_id").isNull()) \
    .count()

payment_null_order_id = payments_df \
    .filter(col("order_id").isNull()) \
    .count()

payment_null_date = payments_df \
    .filter(col("payment_date").isNull()) \
    .count()

payment_null_amount = payments_df \
    .filter(col("amount").isNull()) \
    .count()

payment_negative_amount = payments_df \
    .filter(col("amount") < 0) \
    .count()

print(f"Total payment records: {payment_record_count}")
print(f"Duplicate payment IDs: {payment_duplicate_count}")
print(f"NULL payment IDs: {payment_null_id}")
print(f"NULL order IDs: {payment_null_order_id}")
print(f"NULL payment dates: {payment_null_date}")
print(f"NULL payment amounts: {payment_null_amount}")
print(f"Negative payment amounts: {payment_negative_amount}")

# ---------------------------------------------------------
# Overall Data Quality Status
# ---------------------------------------------------------

quality_checks = [
    customer_duplicate_count,
    customer_null_id,
    customer_null_name,
    customer_null_signup_date,

    product_duplicate_count,
    product_null_id,
    product_null_name,
    product_null_price,
    product_negative_price,

    order_duplicate_count,
    order_null_id,
    order_null_customer_id,
    order_null_product_id,
    order_null_date,
    order_invalid_quantity,
    order_invalid_price,

    payment_duplicate_count,
    payment_null_id,
    payment_null_order_id,
    payment_null_date,
    payment_null_amount,
    payment_negative_amount
]

if all(check == 0 for check in quality_checks):
    print("\nDATA QUALITY STATUS: PASSED")
else:
    print("\nDATA QUALITY STATUS: FAILED")

# ---------------------------------------------------------
# Stop Spark Session
# ---------------------------------------------------------

spark.stop()
