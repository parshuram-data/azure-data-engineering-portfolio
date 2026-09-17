from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round

spark = SparkSession.builder \
    .appName("Retail-Silver-to-Gold") \
    .getOrCreate()

# Read cleaned Silver data
customers = spark.read.option("header", True).option("inferSchema", True).csv(
    "data/customers.csv"
)

products = spark.read.option("header", True).option("inferSchema", True).csv(
    "data/products.csv"
)

orders = spark.read.option("header", True).option("inferSchema", True).csv(
    "data/orders.csv"
)

# Keep completed orders only
completed_orders = orders.filter(
    col("status") == "Completed"
)

# Create order-level revenue
order_revenue = completed_orders.withColumn(
    "order_value",
    col("quantity") * col("unit_price")
)

# Gold: Customer sales
customer_sales = order_revenue.groupBy(
    "customer_id"
).agg(
    sum("order_value").alias("total_sales"),
    count("order_id").alias("total_orders")
)

# Join customer details
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

# Gold: Product sales
product_sales = order_revenue.groupBy(
    "product_id"
).agg(
    sum("quantity").alias("units_sold"),
    sum("order_value").alias("revenue")
)

# Join product details
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

# Display Gold datasets
print("Customer Sales:")
gold_customer_sales.show()

print("Product Sales:")
gold_product_sales.show()

print("Silver-to-Gold transformation completed successfully.")

spark.stop()
