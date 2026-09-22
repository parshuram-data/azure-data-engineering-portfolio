
# Azure Data Factory Pipeline

This folder contains the Azure Data Factory orchestration design for the Azure Retail Data Platform.

## Pipeline Overview

Azure Data Factory orchestrates the end-to-end data engineering workflow.

### Data Flow

Source Systems
→ Azure Data Factory
→ ADLS Gen2 Bronze
→ Databricks / PySpark
→ ADLS Gen2 Silver
→ Data Quality Checks
→ ADLS Gen2 Gold
→ Azure Synapse Analytics
→ Power BI

## Pipeline Definition

Main pipeline:

`PL_Retail_Data_Engineering`

Definition: [`pipeline_retail_data.json`](./pipeline_retail_data.json)

Parameters:

- `sourceFolder`
- `bronzeFolder`
- `silverFolder`
- `goldFolder`

These parameters allow reusable storage paths across environments.

## Metadata-Driven Processing

Metadata configuration:

[`metadata/pipeline_config.csv`](./metadata/pipeline_config.csv)

The metadata contains:

- Source name
- Source type
- Source path
- Target path
- Load type
- Watermark column
- Target layer
- Active flag

### Processing Pattern

pipeline_config.csv
→ Lookup_Source_Metadata
→ ForEach_Source_Table
→ Dynamic source processing
→ Copy_To_Bronze

This approach avoids hard-coding individual source datasets and makes it easier to add new sources.

## ADF Pipeline Activities

### 1. Lookup Source Metadata

`Lookup_Source_Metadata`

- Reads source metadata.
- Determines datasets to process.
- Passes the metadata collection to the ForEach activity.

### 2. ForEach Source Table

`ForEach_Source_Table`

- Iterates through multiple datasets.
- Enables reusable, metadata-driven processing.

### 3. Copy to Bronze

`Copy_To_Bronze`

- Copies raw source data into ADLS Gen2 Bronze.
- Preserves the raw source data.

### 4. Bronze to Silver

`Bronze_To_Silver_Databricks`

- Invokes a Databricks notebook.
- Performs PySpark cleansing, deduplication and business transformations.

### 5. Data Quality Checks

`Data_Quality_Checks`

Validates:

- Null values
- Duplicate records
- Required columns
- Expected data types
- Data quality rules

### 6. Silver to Gold

`Silver_To_Gold_Databricks`

Creates analytics-ready Gold datasets for reporting and business analysis.

### 7. Pipeline Failure Handling

`Pipeline_Failure_Handling`

Provides a failure-notification integration pattern that can be connected to:

- Azure Logic Apps
- Microsoft Teams
- Email
- Azure Monitor

No production credentials or connection details are stored in the repository.

## Incremental Loading

Incremental loading can use watermark columns such as:

- `last_modified`
- `last_modified_date`
- `created_date`
- `updated_timestamp`

Only new or modified records are processed during incremental runs.

See [`incremental_load.md`](./incremental_load.md).

## Medallion Architecture

### Bronze
Raw source data with minimal transformation.

### Silver
Cleaned, standardized and validated data.

### Gold
Business-ready datasets optimized for analytics and reporting.

## Monitoring and Error Handling

Azure Data Factory Monitor can track:

- Pipeline status
- Activity status
- Execution duration
- Errors and failures
- Retry attempts

Typical troubleshooting flow:

ADF Monitor
→ Identify Failed Activity
→ Review Error
→ Check Databricks Logs
→ Validate Source/Target
→ Correct and Rerun

## Source Data

Sample retail datasets include:

- `customers.csv`
- `products.csv`
- `orders.csv`
- `payments.csv`

Additional datasets can be incorporated through the metadata-driven configuration.

## Repository Files

```text
adf/
├── README.md
├── incremental_load.md
├── pipeline_retail_data.json
└── metadata/
    └── pipeline_config.csv
````

| File                           | Purpose                                          |
| ------------------------------ | ------------------------------------------------ |
| `README.md`                    | ADF architecture and orchestration documentation |
| `pipeline_retail_data.json`    | Representative ADF pipeline definition           |
| `incremental_load.md`          | Incremental loading strategy                     |
| `metadata/pipeline_config.csv` | Metadata configuration                           |

## Technologies

* Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure Databricks
* PySpark
* Delta Lake
* Azure Synapse Analytics
* Power BI
