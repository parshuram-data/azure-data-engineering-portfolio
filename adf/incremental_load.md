
# Incremental Loading Design

## Overview

Incremental loading is a common data engineering pattern used to process
only new or changed records instead of repeatedly processing an entire
source dataset.

This repository demonstrates the configuration and orchestration pattern
for incremental loading using Azure Data Factory metadata.

The current sample does **not** include a persistent production watermark
store or a fully dynamic source query that filters records using the
watermark.

---

## Metadata Configuration

Incremental loading information is maintained in:

```text
adf/metadata/pipeline_config.csv
````

The relevant metadata fields are:

| Field             | Purpose                                                                  |
| ----------------- | ------------------------------------------------------------------------ |
| `LoadType`        | Defines whether a source follows Full or Incremental loading             |
| `WatermarkColumn` | Identifies the source column that can be used for incremental processing |

Example:

| Source    | Load Type   | Watermark Column |
| --------- | ----------- | ---------------- |
| customers | Incremental | signup_date      |
| orders    | Incremental | order_date       |
| products  | Full        | —                |
| payments  | Incremental | payment_date     |

---

## Incremental Loading Pattern

A production implementation can follow this flow:

```text
              Source System
                   │
                   ▼
          Read Last Watermark
                   │
                   ▼
        Identify New / Changed
               Records
                   │
                   ▼
            Azure Data Factory
                   │
                   ▼
             Bronze Layer
                   │
                   ▼
             Silver Layer
                   │
                   ▼
               Gold Layer
                   │
                   ▼
          Update Last Watermark
```

---

## Watermark Concept

A watermark represents the point up to which source data has already been
processed.

For example, if the source contains:

```text
order_date
----------
2024-06-20
2024-06-21
2024-06-22
2024-06-23
```

and the previously processed watermark is:

```text
2024-06-22
```

the next ingestion could request records where:

```sql
order_date > '2024-06-22'
```

This would identify:

```text
2024-06-23
```

for processing.

---

## Production Watermark Store

A production implementation would normally maintain the latest successful
watermark in a persistent control table or metadata store.

Example conceptual structure:

| SourceName | WatermarkColumn | LastWatermark | LastRunStatus |
| ---------- | --------------- | ------------- | ------------- |
| customers  | signup_date     | 2024-06-02    | Success       |
| orders     | order_date      | 2024-06-23    | Success       |
| payments   | payment_date    | 2024-06-23    | Success       |

The watermark should normally be updated only after the corresponding
ingestion succeeds.

---

## Failure Handling

A robust incremental pipeline should avoid advancing the watermark when
processing fails.

Example:

```text
Read watermark
      │
      ▼
Extract incremental data
      │
      ▼
Process data
      │
   ┌──┴──┐
   │     │
Success Failure
   │     │
   ▼     ▼
Update   Keep
watermark watermark
```

This prevents records from being skipped during a failed pipeline run.

---

## Late-Arriving Records

For production systems, using a simple:

```sql
WHERE watermark_column > last_watermark
```

condition may not be sufficient when source records can arrive late or be
updated after their original timestamp.

Possible approaches include:

* Overlapping extraction windows
* Source-system change tracking
* CDC
* Change Tracking
* Modified timestamps
* Reprocessing recent partitions

The appropriate approach depends on the source system and business
requirements.

---

## Full vs Incremental Loading

The metadata configuration supports both patterns.

### Full Load

```text
Source
  ↓
Read complete dataset
  ↓
Bronze
```

Currently configured for:

```text
products
```

### Incremental Load

```text
Source
  ↓
Read watermark
  ↓
Extract new / changed records
  ↓
Bronze
```

Currently represented for:

```text
customers
orders
payments
```

---

## Current Repository Scope

The repository currently implements:

* Metadata containing `LoadType`
* Metadata containing `WatermarkColumn`
* ADF metadata lookup pattern
* ADF ForEach orchestration pattern
* Parameterized Bronze/Silver/Gold paths

The repository does not currently implement:

* Persistent watermark storage
* Dynamic watermark query generation
* Automatic watermark updates
* CDC
* Change Tracking
* Production source-system connectors

These are documented as potential production extensions rather than
implemented functionality.

---

## Key Takeaway

The portfolio demonstrates the **architecture and configuration pattern**
for incremental loading while keeping the implementation scope explicit.

A production implementation would extend this pattern with a persistent
watermark store, dynamic source filtering and reliable watermark updates.
