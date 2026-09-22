
# Azure Data Factory

## Overview

Azure Data Factory is used as the orchestration layer for the Azure Data
Engineering platform.

The repository demonstrates a metadata-driven orchestration pattern that
coordinates ingestion, Databricks transformations, data-quality validation
and Gold-layer processing.

---

## Pipeline Flow

```text
Source CSV Files
       │
       ▼
Lookup_Source_Metadata
       │
       ▼
ForEach_Source_Table
       │
       ▼
Copy_To_Bronze
       │
       ▼
Bronze_To_Silver_Databricks
       │
       ▼
Data_Quality_Checks
       │
       ▼
Silver_To_Gold_Databricks
       │
       ▼
Analytics / Serving Layer
````

---

## Pipeline

Pipeline name:

```text
PL_Retail_Data_Engineering
```

The pipeline is represented in:

```text
adf/pipeline_retail_data.json
```

---

## Pipeline Parameters

The current pipeline defines the following parameters:

| Parameter      | Purpose               | Default  |
| -------------- | --------------------- | -------- |
| `bronzeFolder` | Bronze layer location | `bronze` |
| `silverFolder` | Silver layer location | `silver` |
| `goldFolder`   | Gold layer location   | `gold`   |

These parameters are passed to Databricks notebooks so that the
transformation layer is not tied to a single hard-coded environment path.

---

## Metadata-Driven Design

Source configuration is maintained in:

```text
adf/metadata/pipeline_config.csv
```

The metadata contains:

| Column            | Purpose                                    |
| ----------------- | ------------------------------------------ |
| `SourceName`      | Logical source dataset name                |
| `SourceType`      | Source format                              |
| `SourcePath`      | Source location                            |
| `TargetPath`      | Bronze target location                     |
| `LoadType`        | Full or incremental loading pattern        |
| `WatermarkColumn` | Column intended for incremental processing |
| `TargetLayer`     | Target Medallion layer                     |
| `IsActive`        | Indicates whether the source is active     |

Example configuration:

```text
customers → Incremental → signup_date
orders    → Incremental → order_date
products  → Full
payments  → Incremental → payment_date
```

The configuration provides a reusable metadata structure for source
orchestration.

---

## Bronze Ingestion

The ingestion stage is represented by:

```text
Lookup_Source_Metadata
        ↓
ForEach_Source_Table
        ↓
Copy_To_Bronze
```

The Bronze layer is intended to preserve source data before business
transformations are applied.

Expected Bronze datasets:

```text
bronze/
├── customers/
├── products/
├── orders/
└── payments/
```

---

## Databricks Integration

After Bronze ingestion, Azure Data Factory invokes Databricks notebooks.

### Bronze → Silver

```text
Bronze_To_Silver_Databricks
```

Notebook:

```text
/Shared/azure-data-engineering-portfolio/bronze_to_silver
```

The notebook performs:

* Data type conversion
* String trimming
* Status standardization
* Category standardization
* Basic validation
* Deduplication
* Delta Lake writes

---

## Data Quality

The pipeline then invokes:

```text
Data_Quality_Checks
```

Notebook:

```text
/Shared/azure-data-engineering-portfolio/data_quality_checks
```

The validation layer checks:

* Duplicate identifiers
* NULL identifiers
* NULL required attributes
* Invalid quantities
* Invalid prices
* Invalid payment amounts

The quality process returns a passed or failed status based on the
configured checks.

---

## Silver → Gold

After successful data-quality validation:

```text
Silver_To_Gold_Databricks
```

Notebook:

```text
/Shared/azure-data-engineering-portfolio/silver_to_gold
```

The transformation creates the analytics-ready Gold datasets:

```text
gold/
├── customer_sales/
└── product_sales/
```

Sales calculations use:

```text
order_value = quantity × unit_price
```

Only completed orders are included in sales calculations.

---

## Failure Handling

The pipeline includes:

```text
Pipeline_Failure_Handling
```

This activity represents a failure-notification integration.

The current JSON uses a placeholder endpoint:

```text
https://example.com/ADF-Failure-Notification
```

This is intentionally documented as a placeholder and is **not a live
production notification integration**.

In a production Azure environment this could be connected to services such
as:

* Azure Logic Apps
* Microsoft Teams
* Email notification workflows
* Azure Monitor / alerting

---

## Incremental Loading

The metadata configuration contains:

```text
LoadType
WatermarkColumn
```

These fields support an incremental-loading design.

For example:

```text
orders → order_date
payments → payment_date
customers → signup_date
```

The current repository documents the pattern but does not implement a
persistent watermark store or complete dynamic watermark filtering.

A production implementation could use:

```text
Source
  ↓
Read Last Watermark
  ↓
Filter New / Changed Records
  ↓
Copy to Bronze
  ↓
Process Silver
  ↓
Update Watermark
```

This distinction keeps the portfolio accurate about implemented versus
production-ready functionality.

---

## Medallion Architecture

The ADF orchestration follows the Medallion Architecture:

```text
             ┌──────────────┐
             │    Bronze    │
             │ Raw ingestion│
             └──────┬───────┘
                    │
                    ▼
             ┌──────────────┐
             │    Silver    │
             │ Curated data │
             └──────┬───────┘
                    │
                    ▼
             ┌──────────────┐
             │     Gold     │
             │  Analytics   │
             └──────────────┘
```

---

## Monitoring and Error Handling

In a deployed Azure environment, pipeline monitoring can be performed
through Azure Data Factory monitoring.

Typical operational checks include:

1. Pipeline run status
2. Activity failure details
3. Databricks notebook execution logs
4. Data-quality results
5. Target dataset availability
6. Downstream reporting impact

The repository provides the pipeline structure and validation logic rather
than a live Azure monitoring configuration.

---

## Repository Files

```text
adf/
├── metadata/
│   └── pipeline_config.csv
├── pipeline_retail_data.json
├── README.md
└── incremental_load.md
```

---

## Key Concepts Demonstrated

* Azure Data Factory orchestration
* Metadata-driven pipeline design
* Medallion Architecture
* Parameterized Databricks notebooks
* Bronze → Silver → Gold processing
* Data-quality validation
* Failure-handling patterns
* Incremental-loading design
* Delta Lake integration
* CI/CD validation

---

## Important Implementation Note

The JSON file in this repository is a portfolio representation of the ADF
pipeline orchestration.

A production deployment would additionally require environment-specific:

* Linked services
* Datasets
* Storage credentials / managed identities
* Dynamic source and sink expressions
* Databricks linked services
* Environment parameters
* Monitoring and alerting configuration

````
