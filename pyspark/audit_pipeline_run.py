from datetime import datetime, timezone
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)


# =========================================================
# Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("RetailPipelineRunAudit")
    .getOrCreate()
)


# =========================================================
# Parameters
# =========================================================

dbutils.widgets.text(
    "audit_path",
    "control/pipeline_run_audit"
)

dbutils.widgets.text(
    "run_id",
    ""
)

dbutils.widgets.text(
    "pipeline_name",
    ""
)

dbutils.widgets.text(
    "source_name",
    "RETAIL_PIPELINE"
)

dbutils.widgets.text(
    "load_type",
    "MULTI_SOURCE"
)

dbutils.widgets.text(
    "status",
    ""
)

dbutils.widgets.text(
    "start_time",
    ""
)

dbutils.widgets.text(
    "end_time",
    ""
)

dbutils.widgets.text(
    "metrics_json",
    "{}"
)

dbutils.widgets.text(
    "watermark_start",
    ""
)

dbutils.widgets.text(
    "watermark_end",
    ""
)

dbutils.widgets.text(
    "error_message",
    ""
)


# =========================================================
# Read parameters
# =========================================================

audit_path = dbutils.widgets.get(
    "audit_path"
).strip()

run_id = dbutils.widgets.get(
    "run_id"
).strip()

pipeline_name = dbutils.widgets.get(
    "pipeline_name"
).strip()

source_name = dbutils.widgets.get(
    "source_name"
).strip()

load_type = dbutils.widgets.get(
    "load_type"
).strip()

status = dbutils.widgets.get(
    "status"
).strip()

start_time = dbutils.widgets.get(
    "start_time"
).strip()

end_time = dbutils.widgets.get(
    "end_time"
).strip()

metrics_json = dbutils.widgets.get(
    "metrics_json"
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
# Validate required parameters
# =========================================================

if not audit_path:
    raise ValueError(
        "audit_path cannot be empty"
    )

if not run_id:
    raise ValueError(
        "run_id cannot be empty"
    )

if not pipeline_name:
    raise ValueError(
        "pipeline_name cannot be empty"
    )

if not status:
    raise ValueError(
        "status cannot be empty"
    )


# =========================================================
# Parse metrics JSON
# =========================================================

try:

    metrics = json.loads(
        metrics_json
    )

except json.JSONDecodeError as exc:

    raise ValueError(
        "metrics_json must contain valid JSON. "
        f"Received: {metrics_json}"
    ) from exc


# =========================================================
# Safely extract metrics
# =========================================================

source_record_count = int(
    metrics.get(
        "source_record_count",
        0
    )
)

bronze_record_count = int(
    metrics.get(
        "bronze_record_count",
        0
    )
)

silver_record_count = int(
    metrics.get(
        "silver_record_count",
        0
    )
)

rejected_record_count = int(
    metrics.get(
        "rejected_record_count",
        0
    )
)


# =========================================================
# Audit timestamp
# =========================================================

audit_recorded_at = datetime.now(
    timezone.utc
).strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)


# =========================================================
# Audit schema
# =========================================================

audit_schema = StructType([
    StructField(
        "RunId",
        StringType(),
        False
    ),
    StructField(
        "PipelineName",
        StringType(),
        False
    ),
    StructField(
        "SourceName",
        StringType(),
        False
    ),
    StructField(
        "LoadType",
        StringType(),
        True
    ),
    StructField(
        "StartTime",
        StringType(),
        True
    ),
    StructField(
        "EndTime",
        StringType(),
        True
    ),
    StructField(
        "Status",
        StringType(),
        False
    ),
    StructField(
        "SourceRecordCount",
        LongType(),
        True
    ),
    StructField(
        "BronzeRecordCount",
        LongType(),
        True
    ),
    StructField(
        "SilverRecordCount",
        LongType(),
        True
    ),
    StructField(
        "RejectedRecordCount",
        LongType(),
        True
    ),
    StructField(
        "WatermarkStart",
        StringType(),
        True
    ),
    StructField(
        "WatermarkEnd",
        StringType(),
        True
    ),
    StructField(
        "ErrorMessage",
        StringType(),
        True
    ),
    StructField(
        "AuditRecordedAt",
        StringType(),
        False
    )
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
    source_record_count,
    bronze_record_count,
    silver_record_count,
    rejected_record_count,
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

print(
    f"Run ID               : {run_id}"
)

print(
    f"Pipeline             : {pipeline_name}"
)

print(
    f"Source               : {source_name}"
)

print(
    f"Load Type            : {load_type}"
)

print(
    f"Status               : {status}"
)

print(
    f"Source Records       : {source_record_count}"
)

print(
    f"Bronze Records       : {bronze_record_count}"
)

print(
    f"Silver Records       : {silver_record_count}"
)

print(
    f"Rejected Records     : {rejected_record_count}"
)

print(
    f"Watermark Start      : {watermark_start}"
)

print(
    f"Watermark End        : {watermark_end}"
)

print(
    f"Audit Recorded At    : {audit_recorded_at}"
)

if error_message:

    print(
        f"Error Message        : {error_message}"
    )

print(
    f"Audit Delta Path     : {audit_path}"
)

print("=" * 80)
print(
    "PIPELINE RUN AUDIT WRITTEN SUCCESSFULLY"
)
print("=" * 80)


# =========================================================
# Return result to ADF
# =========================================================

result = {
    "run_id": run_id,
    "pipeline_name": pipeline_name,
    "status": status,
    "audit_path": audit_path,
    "source_record_count": source_record_count,
    "bronze_record_count": bronze_record_count,
    "silver_record_count": silver_record_count,
    "rejected_record_count": rejected_record_count
}

dbutils.notebook.exit(
    json.dumps(result)
)
