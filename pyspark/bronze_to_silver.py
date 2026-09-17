from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Retail-Bronze-to-Silver") \
    .getOrCreate()

# Bronze layer paths
customers_path = "data/customers.csv"
products_path = "data/products.csv"
orders_path = "data/orders.csv"
payments_path = "data/payments.csv"

# Read raw data
customers_df = spark.read.option("header", True).option("inferSchema", True).csv(customers_path)
products_df = spark.read.option("header", True).option("inferSchema", True).csv(products_path)
orders_df = spark.read.option("header", True).option("inferSchema", True).csv(orders_path)
payments_df = spark.read.option("header", True).option("inferSchema", True).csv(payments_path)

# Clean customers
customers_silver = customers_df \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("city", trim(col("city"))) \
    .withColumn("state", trim(col("state"))) \
    .withColumn("signup_date", to_date(col("signup_date"))) \
    .dropDuplicates(["customer_id"])

# Clean products
products_silver = products_df \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("category", upper(trim(col("category")))) \
    .dropDuplicates(["product_id"])

# Clean orders
orders_silver = orders_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("status", upper(trim(col("status")))) \
    .filter(col("quantity") > 0) \
    .dropDuplicates(["order_id"])

# Clean payments
payments_silver = payments_df \
    .withColumn("payment_date", to_date(col("payment_date"))) \
    .withColumn("payment_method", upper(trim(col("payment_method")))) \
    .withColumn("payment_status", upper(trim(col("payment_status")))) \
    .filter(col("amount") >= 0) \
    .dropDuplicates(["payment_id"])

# Display results
customers_silver.show()
products_silver.show()
orders_silver.show()
payments_silver.show()

print("Bronze-to-Silver transformation completed successfully.")

spark.stop()
