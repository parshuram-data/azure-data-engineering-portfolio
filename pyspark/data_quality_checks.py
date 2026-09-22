from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# ---------------------------------------------------------
# Spark Session
# ---------------------------------------------------------

spark = SparkSession.builder \
    .appName("Retail-Data-Quality-Checks") \
    .getOrCreate()

# ---------------------------------------------------------
# Databricks Notebook Parameter
# ---------------------------------------------------------
# ADF passes:
# input_path = @pipeline().parameters.silverFolder
#
# Example:
# /mnt/retail/silver
# ---------------------------------------------------------

dbutils.widgets.text("input_path", "data/silver/customers")

silver_path = dbutils.widgets.get("input_path")

print(f"Silver input path: {silver_path}")

# ---------------------------------------------------------
# Read Silver Data
# ---------------------------------------------------------

df = spark.read \
    .format("delta") \
    .load(f"{silver_path}/customers")

print("===== DATA QUALITY CHECKS =====")

# ---------------------------------------------------------
# 1. Record Count
# ---------------------------------------------------------

record_count = df.count()

print(f"Total records: {record_count}")

# ---------------------------------------------------------
# 2. Duplicate Customer IDs
# ---------------------------------------------------------

duplicate_groups = df.groupBy("customer_id") \
    .count() \
    .filter(col("count") > 1)

duplicate_count = duplicate_groups.count()

print(f"Duplicate customer ID groups: {duplicate_count}")

# ---------------------------------------------------------
# 3. NULL Customer IDs
# ---------------------------------------------------------

null_customer_id = df.filter(
    col("customer_id").isNull()
).count()

print(f"NULL customer IDs: {null_customer_id}")

# ---------------------------------------------------------
# 4. NULL Customer Names
# ---------------------------------------------------------

null_customer_name = df.filter(
    col("customer_name").isNull() |
    (trim(col("customer_name")) == "")
).count()

print(f"NULL/blank customer names: {null_customer_name}")

# ---------------------------------------------------------
# 5. Invalid Email Values
# ---------------------------------------------------------
# NULL and blank emails are treated as invalid.

invalid_email = df.filter(
    col("email").isNull() |
    (trim(col("email")) == "") |
    (~col("email").contains("@"))
).count()

print(f"Invalid email records: {invalid_email}")

# ---------------------------------------------------------
# 6. Overall Data Quality Status
# ---------------------------------------------------------

quality_passed = (
    duplicate_count == 0
    and null_customer_id == 0
    and null_customer_name == 0
    and invalid_email == 0
)

if quality_passed:

    print("DATA QUALITY STATUS: PASSED")

else:

    print("DATA QUALITY STATUS: FAILED")

    # Fail the notebook so that ADF can detect the failure.
    raise Exception(
        "Data quality checks failed. "
        "Review the validation results above."
    )

spark.stop()
