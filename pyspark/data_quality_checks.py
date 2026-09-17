from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Create Spark session
spark = SparkSession.builder \
    .appName("Data Quality Checks") \
    .getOrCreate()

# Silver layer input path
silver_path = "data/silver/customers"

# Read Silver data
df = spark.read.format("delta").load(silver_path)

print("===== DATA QUALITY CHECKS =====")

# 1. Record count
record_count = df.count()
print(f"Total records: {record_count}")

# 2. Check duplicate records
duplicate_count = df.groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

print(f"Duplicate customer IDs: {duplicate_count}")

# 3. Check NULL customer IDs
null_customer_id = df.filter(
    col("customer_id").isNull()
).count()

print(f"NULL customer IDs: {null_customer_id}")

# 4. Check NULL customer names
null_customer_name = df.filter(
    col("customer_name").isNull()
).count()

print(f"NULL customer names: {null_customer_name}")

# 5. Check invalid email values
invalid_email = df.filter(
    ~col("email").contains("@")
).count()

print(f"Invalid email records: {invalid_email}")

# 6. Overall data quality status
if (
    duplicate_count == 0
    and null_customer_id == 0
    and null_customer_name == 0
    and invalid_email == 0
):
    print("DATA QUALITY STATUS: PASSED")
else:
    print("DATA QUALITY STATUS: FAILED")

spark.stop()
