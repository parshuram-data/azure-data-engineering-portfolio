from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    to_timestamp,
    lit
)
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Retail-Bronze-to-Silver")
    .getOrCreate()
)


# ============================================================
# ADF Parameters
# ============================================================

dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_path", "silver")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("load_type", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("last_successful_watermark", "")

bronze_source_path = dbutils.widgets.get("source_path")
silver_path = dbutils.widgets.get("target_path")
source_name = dbutils.widgets.get("source_name").strip().lower()
load_type = dbutils.widgets.get("load_type").strip().lower()
watermark_column = dbutils.widgets.get("watermark_column").strip()
last_successful_watermark = dbutils.widgets.get(
    "last_successful_watermark"
).strip()


print("============================================================")
print("Retail Bronze → Silver Processing")
print("============================================================")
print(f"Source Name                : {source_name}")
print(f"Load Type                  : {load_type}")
print(f"Watermark Column           : {watermark_column}")
print(f"Last Successful Watermark  : {last_successful_watermark}")
print(f"Bronze Source Path         : {bronze_source_path}")
print(f"Silver Target Path         : {silver_path}")
print("============================================================")


# ============================================================
# Validation
# ============================================================

supported_sources = {
    "customers",
    "orders",
    "products",
    "payments"
}

supported_load_types = {
    "full",
    "incremental"
}

if source_name not in supported_sources:
    raise ValueError(
        f"Unsupported source_name: {source_name}. "
        f"Supported sources: {sorted(supported_sources)}"
    )

if load_type not in supported_load_types:
    raise ValueError(
        f"Unsupported load_type: {load_type}. "
        f"Supported load types: {sorted(supported_load_types)}"
    )

if load_type == "incremental":
    if not watermark_column:
        raise ValueError(
            f"Watermark column is required for incremental source: "
            f"{source_name}"
        )

    if not last_successful_watermark:
        raise ValueError(
            f"Last successful watermark is required for incremental source: "
            f"{source_name}"
        )


# ============================================================
# Source Configuration
# ============================================================

source_config = {
    "customers": {
        "primary_key": "customer_id",
        "date_columns": ["signup_date"]
    },
    "products": {
        "primary_key": "product_id",
        "date_columns": []
    },
    "orders": {
        "primary_key": "order_id",
        "date_columns": ["order_date"]
    },
    "payments": {
        "primary_key": "payment_id",
        "date_columns": ["payment_date"]
    }
}


config = source_config[source_name]
primary_key = config["primary_key"]


# ============================================================
# Read Current Source from Bronze
# ============================================================

input_file = f"{bronze_source_path}/{source_name}.csv"

print(f"Reading Bronze file: {input_file}")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_file)
)


source_record_count = df.count()

print(f"Bronze source record count: {source_record_count}")


if source_record_count == 0:
    raise ValueError(
        f"Bronze source contains zero records: {input_file}"
    )


# ============================================================
# Apply Incremental Watermark
# ============================================================

if load_type == "incremental":

    print(
        f"Applying incremental filter: "
        f"{watermark_column} > {last_successful_watermark}"
    )

    if watermark_column not in df.columns:
        raise ValueError(
            f"Watermark column '{watermark_column}' was not found "
            f"in source '{source_name}'. "
            f"Available columns: {df.columns}"
        )

    df = df.withColumn(
        "__watermark_value",
        to_timestamp(col(watermark_column))
    )

    watermark_value = to_timestamp(
        lit(last_successful_watermark)
    )

    df = (
        df
        .filter(
            col("__watermark_value") > watermark_value
        )
        .drop("__watermark_value")
    )

    filtered_record_count = df.count()

    print(
        f"Records after watermark filtering: "
        f"{filtered_record_count}"
    )

else:

    print("Full load selected. No watermark filter applied.")

    filtered_record_count = df.count()


# ============================================================
# Handle Empty Incremental Result
# ============================================================

if filtered_record_count == 0:

    print(
        f"No new records found for source '{source_name}'. "
        "Silver table will not be modified."
    )

    print("============================================================")
    print("Bronze → Silver processing completed with no new records.")
    print("============================================================")

    spark.stop()

else:

    # ========================================================
    # Source-Specific Transformations
    # ========================================================

    if source_name == "customers":

        silver_df = (
            df
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
            .dropDuplicates(["customer_id"])
        )


    elif source_name == "products":

        silver_df = (
            df
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
                col("price").cast(DoubleType())
            )
            .filter(
                col("price") >= 0
            )
            .dropDuplicates(["product_id"])
        )


    elif source_name == "orders":

        silver_df = (
            df
            .withColumn(
                "order_date",
                to_date(col("order_date"))
            )
            .withColumn(
                "status",
                upper(trim(col("status")))
            )
            .withColumn(
                "quantity",
                col("quantity").cast(IntegerType())
            )
            .withColumn(
                "unit_price",
                col("unit_price").cast(DoubleType())
            )
            .filter(
                col("quantity") > 0
            )
            .filter(
                col("unit_price") >= 0
            )
            .dropDuplicates(["order_id"])
        )


    elif source_name == "payments":

        silver_df = (
            df
            .withColumn(
                "payment_date",
                to_date(col("payment_date"))
            )
            .withColumn(
                "payment_method",
                upper(trim(col("payment_method")))
            )
            .withColumn(
                "payment_status",
                upper(trim(col("payment_status")))
            )
            .withColumn(
                "amount",
                col("amount").cast(DoubleType())
            )
            .filter(
                col("amount") >= 0
            )
            .dropDuplicates(["payment_id"])
        )


    # ========================================================
    # Silver Target
    # ========================================================

    target_path = f"{silver_path}/{source_name}"

    silver_record_count = silver_df.count()

    print(
        f"Silver records after transformation: "
        f"{silver_record_count}"
    )

    if silver_record_count == 0:

        raise ValueError(
            f"Source '{source_name}' produced zero valid Silver records."
        )


    # ========================================================
    # Write Strategy
    #
    # Full:
    #   Replace target.
    #
    # Incremental:
    #   MERGE using source primary key.
    #   Existing records are updated.
    #   New records are inserted.
    # ========================================================

    if load_type == "full":

        print(
            f"Executing FULL load into Silver: {target_path}"
        )

        (
            silver_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

    else:

        print(
            f"Executing INCREMENTAL MERGE into Silver: "
            f"{target_path}"
        )

        if DeltaTable.isDeltaTable(
            spark,
            target_path
        ):

            delta_table = DeltaTable.forPath(
                spark,
                target_path
            )

            (
                delta_table.alias("target")
                .merge(
                    silver_df.alias("source"),
                    f"target.{primary_key} = "
                    f"source.{primary_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        else:

            print(
                "Silver Delta table does not exist. "
                "Creating initial Delta table."
            )

            (
                silver_df
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )


    # ========================================================
    # Final Counts
    # ========================================================

    final_record_count = (
        spark.read
        .format("delta")
        .load(target_path)
        .count()
    )

    print("============================================================")
    print("Bronze → Silver processing completed successfully.")
    print(f"Source                  : {source_name}")
    print(f"Load Type               : {load_type}")
    print(f"Records Read            : {source_record_count}")
    print(f"Records After Watermark : {filtered_record_count}")
    print(f"Records Transformed     : {silver_record_count}")
    print(f"Final Silver Records    : {final_record_count}")
    print(f"Silver Path             : {target_path}")
    print("============================================================")


spark.stop()
