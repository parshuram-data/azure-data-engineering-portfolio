from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)
from delta.tables import DeltaTable
import json


# ============================================================
# Spark Session
# ============================================================

spark = SparkSession.builder.appName("RetailSourceRunAudit").getOrCreate()


# ============================================================
# Databricks Widgets
# ============================================================

dbutils.widgets.text(
    "audit_path",
    "control/pipeline_run_audit",
    "Audit Delta Path"
)

dbutils.widgets.text(
    "run_id",
    "",
    "Pipeline Run ID"
)

dbutils.widgets.text(
    "pipeline_name",
    "pl_retail_data",
    "Pipeline Name"
)

dbutils.widgets.text(
    "source_name",
    "",
    "Source Name"
)

dbutils.widgets.text(
    "load_type",
    "",
    "Load Type"
)

dbutils.widgets.text(
    "status",
    "SUCCESS",
    "Status"
)

dbutils.widgets.text(
    "start_time",
    "",
    "Start Time"
)

dbutils.widgets.text(
    "end_time",
    "",
    "End Time"
)

dbutils.widgets.text(
    "metrics_json",
    "{}",
    "Metrics JSON"
)

dbutils.widgets.text(
    "error_message",
    "",
    "Error Message"
)


# ============================================================
# Read Parameters
# ============================================================

audit_path = dbutils.widgets.get("audit_path").strip()
run_id = dbutils.widgets.get("run_id").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()
source_name = dbutils.widgets.get("source_name").strip()
load_type = dbutils.widgets.get("load_type").strip()
status = dbutils.widgets.get("status").strip().upper()
start_time = dbutils.widgets.get("start_time").strip()
end_time = dbutils.widgets.get("end_time").strip()
metrics_json = dbutils.widgets.get("metrics_json").strip()
error_message = dbutils.widgets.get("error_message").strip()


# ============================================================
# Validate Required Parameters
# ============================================================

if not audit_path:
    raise ValueError("audit_path is required.")

if not run_id:
    raise ValueError("run_id is required.")

if not pipeline_name:
    raise ValueError("pipeline_name is required.")

if not source_name:
    raise ValueError("source_name is required.")

if not load_type:
    raise ValueError("load_type is required.")

if status not in {"SUCCESS", "FAILED", "PARTIAL"}:
    raise ValueError(
        f"Invalid status '{status}'. "
        "Expected SUCCESS, FAILED, or PARTIAL."
    )


# ============================================================
# Parse Metrics JSON
# ============================================================

try:
    metrics = json.loads(metrics_json) if metrics_json else {}
except json.JSONDecodeError as exc:
    raise ValueError(
        f"metrics_json is not valid JSON: {exc}"
    )


# ============================================================
# Extract Runtime Metrics
# ============================================================

source_record_count = int(
    metrics.get("source_record_count", 0) or 0
)

bronze_record_count = int(
    metrics.get("bronze_record_count", 0) or 0
)

silver_record_count = int(
    metrics.get("silver_record_count", 0) or 0
)

rejected_record_count = int(
    metrics.get("rejected_record_count", 0) or 0
)

records_processed = int(
    metrics.get("records_processed", silver_record_count) or 0
)

old_watermark = str(
    metrics.get("old_watermark", "") or ""
)

new_watermark = str(
    metrics.get("new_watermark", "") or ""
)


# ============================================================
# Build Audit DataFrame
# ============================================================

audit_data = [
    (
        run_id,
        pipeline_name,
        source_name,
        load_type,
        start_time,
        end_time,
        status,
        source_record_count,
        bronze_record_count,
        silver_record_count,
        rejected_record_count,
        old_watermark,
        new_watermark,
        error_message,
        records_processed
    )
]

audit_columns = [
    "RunId",
    "PipelineName",
    "SourceName",
    "LoadType",
    "StartTime",
    "EndTime",
    "Status",
    "SourceRecordCount",
    "BronzeRecordCount",
    "SilverRecordCount",
    "RejectedRecordCount",
    "WatermarkStart",
    "WatermarkEnd",
    "ErrorMessage",
    "RecordsProcessed"
]

audit_df = (
    spark.createDataFrame(
        audit_data,
        audit_columns
    )
    .withColumn(
        "AuditRecordedAt",
        current_timestamp()
    )
)


# ============================================================
# Create Audit Delta Table If It Does Not Exist
# ============================================================

if not DeltaTable.isDeltaTable(spark, audit_path):

    (
        audit_df.write
        .format("delta")
        .mode("overwrite")
        .save(audit_path)
    )

else:

    # ========================================================
    # Idempotent MERGE
    #
    # One source can have only one audit record for a
    # particular pipeline RunId.
    # ========================================================

    audit_table = DeltaTable.forPath(
        spark,
        audit_path
    )

    (
        audit_table.alias("target")
        .merge(
            audit_df.alias("source"),
            """
            target.RunId = source.RunId
            AND target.SourceName = source.SourceName
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ============================================================
# Verify Audit Record
# ============================================================

verification_df = (
    spark.read
    .format("delta")
    .load(audit_path)
    .filter(
        (col("RunId") == run_id) &
        (col("SourceName") == source_name)
    )
)


verification_count = verification_df.count()

if verification_count != 1:
    raise RuntimeError(
        "Audit verification failed. "
        f"Expected exactly 1 record for RunId='{run_id}' "
        f"and SourceName='{source_name}', "
        f"but found {verification_count}."
    )


# ============================================================
# Return Structured Output
# ============================================================

result = {
    "run_id": run_id,
    "source_name": source_name,
    "status": status,
    "source_record_count": source_record_count,
    "bronze_record_count": bronze_record_count,
    "silver_record_count": silver_record_count,
    "rejected_record_count": rejected_record_count,
    "records_processed": records_processed,
    "old_watermark": old_watermark,
    "new_watermark": new_watermark,
    "audit_path": audit_path,
    "message": "Per-source audit record written successfully"
}

dbutils.notebook.exit(
    json.dumps(result)
)
