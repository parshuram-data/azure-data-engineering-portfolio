from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("RetailPipelineRunAudit")
    .getOrCreate()
)

# =========================================================
# Parameters
# =========================================================

dbutils.widgets.text("audit_path", "control/pipeline_run_audit")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("load_type", "")
dbutils.widgets.text("status", "")
dbutils.widgets.text("start_time", "")
dbutils.widgets.text("end_time", "")
dbutils.widgets.text("source_record_count", "0")
dbutils.widgets.text("bronze_record_count", "0")
dbutils.widgets.text("silver_record_count", "0")
dbutils.widgets.text("rejected_record_count", "0")
dbutils.widgets.text("watermark_start", "")
dbutils.widgets.text("watermark_end", "")
dbutils.widgets.text("error_message", "")

audit_path = dbutils.widgets.get("audit_path").strip()
run_id = dbutils.widgets.get("run_id").strip()
pipeline_name = dbutils.widgets.get("pipeline_name").strip()
source_name = dbutils.widgets.get("source_name").strip()
load_type = dbutils.widgets.get("load_type").strip()
status = dbutils.widgets.get("status").strip()
start_time = dbutils.widgets.get("start_time").strip()
end_time = dbutils.widgets.get("end_time").strip()

source_record_count = dbutils.widgets.get(
    "source_record_count"
).strip()

bronze_record_count = dbutils.widgets.get(
    "bronze_record_count"
).strip()

silver_record_count = dbutils.widgets.get(
    "silver_record_count"
).strip()

rejected_record_count = dbutils.widgets.get(
    "rejected_record_count"
).strip()

watermark_start = dbutils.widgets.get(
    "watermark_start"
).strip()

watermark_end = dbutils.widgets.get(
    "watermark_end"
).strip()

error_message = dbutils.widgets.get(
    "error_message"
).strip()

# =========================================================
# Validation
# =========================================================

if not audit_path:
    raise ValueError("audit_path cannot be empty")

if not run_id:
    raise ValueError("run_id cannot be empty")

if not pipeline_name:
    raise ValueError("pipeline_name cannot be empty")

if not source_name:
    raise ValueError("source_name cannot be empty")

if not status:
    raise ValueError("status cannot be empty")


def parse_long(value, field_name):
    if value is None or value == "":
        return 0

    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"{field_name} must be a valid integer. "
            f"Received: {value}"
        )


source_record_count_value = parse_long(
    source_record_count,
    "source_record_count"
)

bronze_record_count_value = parse_long(
    bronze_record_count,
    "bronze_record_count"
)

silver_record_count_value = parse_long(
    silver_record_count,
    "silver_record_count"
)

rejected_record_count_value = parse_long(
    rejected_record_count,
    "rejected_record_count"
)

# =========================================================
# Audit timestamp
# =========================================================

audit_recorded_at = datetime.now(
    timezone.utc
).strftime("%Y-%m-%dT%H:%M:%SZ")

# =========================================================
# Audit schema
# =========================================================

audit_schema = StructType([
    StructField("RunId", StringType(), False),
    StructField("PipelineName", StringType(), False),
    StructField("SourceName", StringType(), False),
    StructField("LoadType", StringType(), True),
    StructField("StartTime", StringType(), True),
    StructField("EndTime", StringType(), True),
    StructField("Status", StringType(), False),
    StructField("SourceRecordCount", LongType(), True),
    StructField("BronzeRecordCount", LongType(), True),
    StructField("SilverRecordCount", LongType(), True),
    StructField("RejectedRecordCount", LongType(), True),
    StructField("WatermarkStart", StringType(), True),
    StructField("WatermarkEnd", StringType(), True),
    StructField("ErrorMessage", StringType(), True),
    StructField("AuditRecordedAt", StringType(), False)
])

# =========================================================
# Create audit record
# =========================================================

audit_record = [(
    run_id,
    pipeline_name,
    source_name,
    load_type,
    start_time,
    end_time,
    status,
    source_record_count_value,
    bronze_record_count_value,
    silver_record_count_value,
    rejected_record_count_value,
    watermark_start,
    watermark_end,
    error_message,
    audit_recorded_at
)]

audit_df = spark.createDataFrame(
    audit_record,
    audit_schema
)

# =========================================================
# Write audit record
# =========================================================

(
    audit_df
    .write
    .format("delta")
    .mode("append")
    .save(audit_path)
)

# =========================================================
# Verification
# =========================================================

print("=" * 80)
print("PIPELINE RUN AUDIT")
print("=" * 80)

print(f"Run ID               : {run_id}")
print(f"Pipeline             : {pipeline_name}")
print(f"Source               : {source_name}")
print(f"Load Type            : {load_type}")
print(f"Status               : {status}")
print(f"Source Records       : {source_record_count_value}")
print(f"Bronze Records       : {bronze_record_count_value}")
print(f"Silver Records       : {silver_record_count_value}")
print(f"Rejected Records     : {rejected_record_count_value}")
print(f"Watermark Start      : {watermark_start}")
print(f"Watermark End        : {watermark_end}")
print(f"Audit Recorded At    : {audit_recorded_at}")

if error_message:
    print(f"Error Message        : {error_message}")

print(f"Audit Delta Path     : {audit_path}")

print("=" * 80)
print("PIPELINE RUN AUDIT WRITTEN SUCCESSFULLY")
print("=" * 80)

# =========================================================
# Return result to ADF
# =========================================================

dbutils.notebook.exit(
    "AUDIT_RECORDED"
)
