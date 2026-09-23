from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    to_timestamp,
    max as spark_max
)
from delta.tables import DeltaTable
import json


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("RetailBronzeToSilver")
    .getOrCreate()
)


# =========================================================
# Parameters
# =========================================================

dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_path", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("load_type", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("last_successful_watermark", "")

source_path = dbutils.widgets.get("source_path").strip()
target_path = dbutils.widgets.get("target_path").strip()
source_name = dbutils.widgets.get("source_name").strip()
load_type = dbutils.widgets.get("load_type").strip().lower()
watermark_column = dbutils.widgets.get("watermark_column").strip()
last_successful_watermark = (
    dbutils.widgets.get("last_successful_watermark").strip()
)


# =========================================================
# Validation
# =========================================================

if not source_path:
    raise ValueError("source_path parameter cannot be empty")

if not target_path:
    raise ValueError("target_path parameter cannot be empty")

if not source_name:
    raise ValueError("source_name parameter cannot be empty")

if load_type not in {"full", "incremental"}:
    raise ValueError(
        f"Unsupported load_type: {load_type}. "
        "Expected 'full' or 'incremental'."
    )


# =========================================================
# Source configuration
# =========================================================

SOURCE_CONFIG = {
    "customers": {
        "primary_key": "customer_id"
    },
    "products": {
        "primary_key": "product_id"
    },
    "orders": {
        "primary_key": "order_id"
    },
    "payments": {
        "primary_key": "payment_id"
    }
}


if source_name not in SOURCE_CONFIG:
    raise ValueError(
        f"Unsupported source: {source_name}. "
        f"Supported sources: {list(SOURCE_CONFIG.keys())}"
    )


primary_key = SOURCE_CONFIG[source_name]["primary_key"]


# =========================================================
# Paths
# =========================================================

bronze_file_path = (
    f"{source_path}/{source_name}.csv"
)

silver_table_path = (
    f"{target_path}/{source_name}"
)


print("=" * 80)
print("BRONZE TO SILVER")
print("=" * 80)
print(f"Source name              : {source_name}")
print(f"Load type                : {load_type}")
print(f"Bronze file              : {bronze_file_path}")
print(f"Silver Delta path        : {silver_table_path}")
print(f"Primary key              : {primary_key}")
print(f"Watermark column         : {watermark_column}")
print(
    f"Last successful watermark: "
    f"{last_successful_watermark}"
)
print("=" * 80)


# =========================================================
# Read Bronze
# =========================================================

bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_file_path)
)

bronze_record_count = bronze_df.count()

print(
    f"Bronze records read: {bronze_record_count}"
)


# =========================================================
# Incremental filtering
# =========================================================

if load_type == "incremental":

    if not watermark_column:
        raise ValueError(
            "watermark_column is required for incremental loads"
        )

    if not last_successful_watermark:
        raise ValueError(
            "last_successful_watermark is required "
            "for incremental loads"
        )

    if watermark_column not in bronze_df.columns:
        raise ValueError(
            f"Watermark column '{watermark_column}' "
            f"not found in Bronze data. "
            f"Available columns: {bronze_df.columns}"
        )

    bronze_df = bronze_df.withColumn(
        "__watermark_value",
        to_timestamp(col(watermark_column))
    )

    watermark_timestamp = to_timestamp(
        col("__watermark_value")
    )

    eligible_df = (
        bronze_df
        .filter(
            col("__watermark_value")
            > to_timestamp(
                __import__("pyspark").sql.functions.lit(
                    last_successful_watermark
                )
            )
        )
    )

    eligible_record_count = eligible_df.count()

else:

    eligible_df = bronze_df.withColumn(
        "__watermark_value",
        (
            to_timestamp(col(watermark_column))
            if watermark_column
            and watermark_column in bronze_df.columns
            else col(primary_key).cast("timestamp")
        )
    )

    eligible_record_count = eligible_df.count()


print(
    f"Eligible records for processing: "
    f"{eligible_record_count}"
)


# =========================================================
# No new incremental records
# =========================================================

if load_type == "incremental" and eligible_record_count == 0:

    print("=" * 80)
    print("NO NEW RECORDS")
    print("=" * 80)
    print(
        "No records were found after the previous "
        "successful watermark."
    )

    result = {
        "source_name": source_name,
        "old_watermark": last_successful_watermark,
        "new_watermark": last_successful_watermark,
        "records_processed": 0,
        "source_record_count": bronze_record_count,
        "bronze_record_count": bronze_record_count,
        "silver_record_count": 0,
        "rejected_record_count": 0,
        "status": "SUCCESS",
        "message": "No new records to process"
    }

    print(json.dumps(result, indent=2))

    dbutils.notebook.exit(
        json.dumps(result)
    )


# =========================================================
# Transformation logic
# =========================================================

transformed_df = eligible_df


# ---------------------------------------------------------
# Customers
# ---------------------------------------------------------

if source_name == "customers":

    transformed_df = (
        transformed_df
        .withColumn(
            "customer_id",
            trim(col("customer_id"))
        )
        .withColumn(
            "customer_name",
            trim(col("customer_name"))
        )
        .withColumn(
            "city",
            trim(col("city"))
        )
        .withColumn(
            "state",
            trim(col("state"))
        )
        .withColumn(
            "signup_date",
            to_date(col("signup_date"))
        )
    )

    transformed_df = transformed_df.filter(
        col("customer_id").isNotNull()
        & (trim(col("customer_id")) != "")
        & col("customer_name").isNotNull()
        & (trim(col("customer_name")) != "")
        & col("signup_date").isNotNull()
    )


# ---------------------------------------------------------
# Products
# ---------------------------------------------------------

elif source_name == "products":

    transformed_df = (
        transformed_df
        .withColumn(
            "product_id",
            trim(col("product_id"))
        )
        .withColumn(
            "product_name",
            trim(col("product_name"))
        )
        .withColumn(
            "category",
            upper(trim(col("category")))
        )
        .withColumn(
            "price",
            col("price").cast("double")
        )
    )

    transformed_df = transformed_df.filter(
        col("product_id").isNotNull()
        & (trim(col("product_id")) != "")
        & col("product_name").isNotNull()
        & (trim(col("product_name")) != "")
        & col("price").isNotNull()
        & (col("price") >= 0)
    )


# ---------------------------------------------------------
# Orders
# ---------------------------------------------------------

elif source_name == "orders":

    transformed_df = (
        transformed_df
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "customer_id",
            trim(col("customer_id"))
        )
        .withColumn(
            "product_id",
            trim(col("product_id"))
        )
        .withColumn(
            "order_date",
            to_date(col("order_date"))
        )
        .withColumn(
            "quantity",
            col("quantity").cast("int")
        )
        .withColumn(
            "unit_price",
            col("unit_price").cast("double")
        )
        .withColumn(
            "status",
            upper(trim(col("status")))
        )
    )

    transformed_df = transformed_df.filter(
        col("order_id").isNotNull()
        & (trim(col("order_id")) != "")
        & col("customer_id").isNotNull()
        & (trim(col("customer_id")) != "")
        & col("product_id").isNotNull()
        & (trim(col("product_id")) != "")
        & col("order_date").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("unit_price").isNotNull()
        & (col("unit_price") >= 0)
        & col("status").isin(
            "COMPLETED",
            "CANCELLED"
        )
    )


# ---------------------------------------------------------
# Payments
# ---------------------------------------------------------

elif source_name == "payments":

    transformed_df = (
        transformed_df
        .withColumn(
            "payment_id",
            trim(col("payment_id"))
        )
        .withColumn(
            "order_id",
            trim(col("order_id"))
        )
        .withColumn(
            "payment_date",
            to_date(col("payment_date"))
        )
        .withColumn(
            "payment_method",
            upper(trim(col("payment_method")))
        )
        .withColumn(
            "amount",
            col("amount").cast("double")
        )
        .withColumn(
            "payment_status",
            upper(trim(col("payment_status")))
        )
    )

    transformed_df = transformed_df.filter(
        col("payment_id").isNotNull()
        & (trim(col("payment_id")) != "")
        & col("order_id").isNotNull()
        & (trim(col("order_id")) != "")
        & col("payment_date").isNotNull()
        & col("amount").isNotNull()
        & (col("amount") >= 0)
        & col("payment_status").isin(
            "PAID",
            "REFUNDED"
        )
    )


# =========================================================
# Deduplication
# =========================================================

transformed_df = transformed_df.drop(
    "__watermark_value"
)

transformed_df = transformed_df.dropDuplicates(
    [primary_key]
)

silver_record_count = transformed_df.count()


# =========================================================
# Rejected record count
# =========================================================

rejected_record_count = max(
    eligible_record_count - silver_record_count,
    0
)


print(
    f"Silver records after transformation: "
    f"{silver_record_count}"
)

print(
    f"Rejected records: "
    f"{rejected_record_count}"
)


# =========================================================
# Fail if all eligible records were rejected
# =========================================================

if eligible_record_count > 0 and silver_record_count == 0:

    raise RuntimeError(
        f"All {eligible_record_count} eligible records "
        "were rejected during Bronze to Silver transformation."
    )


# =========================================================
# Full load
# =========================================================

if load_type == "full":

    print("=" * 80)
    print("FULL LOAD")
    print("=" * 80)

    (
        transformed_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_table_path)
    )

    status = "SUCCESS"

    # Products currently use Full load and have no watermark.
    new_watermark = ""


# =========================================================
# Incremental load
# =========================================================

else:

    print("=" * 80)
    print("INCREMENTAL LOAD")
    print("=" * 80)

    delta_exists = False

    try:
        delta_exists = DeltaTable.isDeltaTable(
            spark,
            silver_table_path
        )
    except Exception:
        delta_exists = False

    if not delta_exists:

        print(
            "Silver Delta table does not exist. "
            "Creating initial Delta table."
        )

        (
            transformed_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_table_path)
        )

    else:

        print(
            "Silver Delta table exists. "
            "Performing idempotent MERGE."
        )

        delta_table = DeltaTable.forPath(
            spark,
            silver_table_path
        )

        merge_condition = (
            f"target.{primary_key} = "
            f"source.{primary_key}"
        )

        (
            delta_table.alias("target")
            .merge(
                transformed_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # -----------------------------------------------------
    # Calculate new watermark from successfully transformed
    # records.
    # -----------------------------------------------------

    if watermark_column not in transformed_df.columns:
        raise ValueError(
            f"Watermark column '{watermark_column}' "
            "not found after transformation."
        )

    new_watermark_row = (
        transformed_df
        .select(
            spark_max(
                to_timestamp(
                    col(watermark_column)
                )
            ).alias("max_watermark")
        )
        .collect()[0]
    )

    new_watermark_value = (
        new_watermark_row["max_watermark"]
    )

    if new_watermark_value is None:

        raise RuntimeError(
            "Unable to determine new watermark "
            "from successfully transformed records."
        )

    new_watermark = (
        new_watermark_value.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    )

    status = "SUCCESS"


# =========================================================
# Final result
# =========================================================

result = {
    "source_name": source_name,
    "old_watermark": last_successful_watermark,
    "new_watermark": new_watermark,
    "records_processed": silver_record_count,
    "source_record_count": bronze_record_count,
    "bronze_record_count": bronze_record_count,
    "silver_record_count": silver_record_count,
    "rejected_record_count": rejected_record_count,
    "status": status,
    "message": (
        "Bronze to Silver processing completed successfully"
    )
}


print("\n" + "=" * 80)
print("BRONZE TO SILVER RESULT")
print("=" * 80)

print(
    json.dumps(
        result,
        indent=2
    )
)

print("=" * 80)


# =========================================================
# Return structured result to ADF
# =========================================================

dbutils.notebook.exit(
    json.dumps(result)
)
