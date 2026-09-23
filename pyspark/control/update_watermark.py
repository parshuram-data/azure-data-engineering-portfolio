from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    max as spark_max
)
from delta.tables import DeltaTable


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Retail-Watermark-Control")
    .getOrCreate()
)


# ============================================================
# Parameters
# ============================================================

dbutils.widgets.text("control_path", "control/watermark_state")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("old_watermark", "")
dbutils.widgets.text("new_watermark", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("records_processed", "0")
dbutils.widgets.text("status", "")


control_path = dbutils.widgets.get("control_path").strip()
source_name = dbutils.widgets.get("source_name").strip().lower()
watermark_column = dbutils.widgets.get("watermark_column").strip()
old_watermark = dbutils.widgets.get("old_watermark").strip()
new_watermark = dbutils.widgets.get("new_watermark").strip()
run_id = dbutils.widgets.get("run_id").strip()
records_processed = int(
    dbutils.widgets.get("records_processed").strip() or "0"
)
status = dbutils.widgets.get("status").strip().upper()


# ============================================================
# Logging
# ============================================================

print("============================================================")
print("Retail Watermark Control")
print("============================================================")
print(f"Source Name        : {source_name}")
print(f"Watermark Column   : {watermark_column}")
print(f"Old Watermark     : {old_watermark}")
print(f"New Watermark     : {new_watermark}")
print(f"Run ID             : {run_id}")
print(f"Records Processed  : {records_processed}")
print(f"Processing Status  : {status}")
print(f"Control Path       : {control_path}")
print("============================================================")


# ============================================================
# Validation
# ============================================================

if not source_name:
    raise ValueError(
        "source_name is required."
    )

if not status:
    raise ValueError(
        "status is required."
    )

if status != "SUCCESS":
    raise ValueError(
        f"Watermark update rejected because processing status "
        f"is '{status}', not 'SUCCESS'."
    )

if not new_watermark:
    raise ValueError(
        "new_watermark is required for a successful watermark update."
    )


# ============================================================
# Control Table Schema
# ============================================================

control_columns = [
    "SourceName",
    "WatermarkColumn",
    "LastSuccessfulWatermark",
    "LastSuccessfulRunId",
    "LastSuccessfulRunTime",
    "Status",
    "RecordsProcessed"
]


# ============================================================
# Create Initial Control Table
# ============================================================

if not DeltaTable.isDeltaTable(
    spark,
    control_path
):

    print(
        "Control Delta table does not exist. "
        "Creating initial control table."
    )

    initial_df = spark.createDataFrame(
        [
            (
                source_name,
                watermark_column,
                new_watermark,
                run_id,
                None,
                "Success",
                records_processed
            )
        ],
        control_columns
    )

    (
        initial_df
        .withColumn(
            "LastSuccessfulRunTime",
            current_timestamp()
        )
        .write
        .format("delta")
        .mode("overwrite")
        .save(control_path)
    )

    print(
        "Initial watermark control record created successfully."
    )

else:

    # ========================================================
    # Existing Control Table
    # ========================================================

    delta_table = DeltaTable.forPath(
        spark,
        control_path
    )


    # ========================================================
    # Read Existing State
    # ========================================================

    existing_df = (
        spark.read
        .format("delta")
        .load(control_path)
        .filter(
            col("SourceName") == source_name
        )
    )


    existing_count = existing_df.count()


    # ========================================================
    # Prevent Watermark Regression
    # ========================================================

    if existing_count > 0:

        current_watermark = (
            existing_df
            .select("LastSuccessfulWatermark")
            .first()[0]
        )

        print(
            f"Existing control watermark: "
            f"{current_watermark}"
        )

        if (
            current_watermark
            and new_watermark < current_watermark
        ):

            raise ValueError(
                f"Watermark regression detected for source "
                f"'{source_name}'. "
                f"Current watermark = {current_watermark}, "
                f"new watermark = {new_watermark}."
            )


    # ========================================================
    # Prepare New State
    # ========================================================

    update_df = spark.createDataFrame(
        [
            (
                source_name,
                watermark_column,
                new_watermark,
                run_id,
                records_processed
            )
        ],
        [
            "SourceName",
            "WatermarkColumn",
            "LastSuccessfulWatermark",
            "LastSuccessfulRunId",
            "RecordsProcessed"
        ]
    )


    # ========================================================
    # Atomic MERGE
    # ========================================================

    print(
        "Updating watermark control state using Delta MERGE."
    )

    (
        delta_table.alias("target")
        .merge(
            update_df.alias("source"),
            "target.SourceName = source.SourceName"
        )
        .whenMatchedUpdate(
            set={
                "WatermarkColumn":
                    "source.WatermarkColumn",

                "LastSuccessfulWatermark":
                    "source.LastSuccessfulWatermark",

                "LastSuccessfulRunId":
                    "source.LastSuccessfulRunId",

                "LastSuccessfulRunTime":
                    "current_timestamp()",

                "Status":
                    "'Success'",

                "RecordsProcessed":
                    "source.RecordsProcessed"
            }
        )
        .whenNotMatchedInsert(
            values={
                "SourceName":
                    "source.SourceName",

                "WatermarkColumn":
                    "source.WatermarkColumn",

                "LastSuccessfulWatermark":
                    "source.LastSuccessfulWatermark",

                "LastSuccessfulRunId":
                    "source.LastSuccessfulRunId",

                "LastSuccessfulRunTime":
                    "current_timestamp()",

                "Status":
                    "'Success'",

                "RecordsProcessed":
                    "source.RecordsProcessed"
            }
        )
        .execute()
    )


# ============================================================
# Verify State
# ============================================================

final_state = (
    spark.read
    .format("delta")
    .load(control_path)
    .filter(
        col("SourceName") == source_name
    )
)

final_state.show(
    truncate=False
)


print("============================================================")
print("Watermark update completed successfully.")
print(f"Source             : {source_name}")
print(f"New Watermark      : {new_watermark}")
print(f"Records Processed  : {records_processed}")
print(f"Run ID             : {run_id}")
print("============================================================")


spark.stop()
