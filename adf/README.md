# Azure Data Factory Pipeline

This folder contains the Azure Data Factory orchestration design for the Azure Retail Data Platform.

## Pipeline Overview

Azure Data Factory is used to orchestrate the end-to-end data engineering workflow.

### Data Flow

Source Systems
→ Azure Data Factory
→ ADLS Gen2 Bronze Layer
→ Databricks PySpark Transformation
→ Silver Layer
→ Gold Layer
→ Azure Synapse Analytics
→ Power BI

## Source Data

The solution uses sample retail datasets:

- customers.csv
- products.csv
- orders.csv
- payments.csv

## ADF Pipeline Activities

The pipeline performs the following activities:

1. **Lookup Activity**
   - Reads table and file metadata.
   - Determines which datasets need to be processed.

2. **ForEach Activity**
   - Iterates through multiple source datasets.
   - Enables reusable and metadata-driven processing.

3. **Copy Activity**
   - Copies source data into the ADLS Gen2 Bronze layer.
   - Preserves the raw source data.

4. **Databricks Activity**
   - Executes PySpark transformations.
   - Performs cleansing, deduplication and business transformations.

5. **Data Quality Checks**
   - Validates null values.
   - Checks duplicate records.
   - Validates required columns and data types.

6. **Gold Layer Processing**
   - Creates analytics-ready datasets.
   - Organizes data for reporting and business analysis.

7. **Error Handling**
   - Pipeline activities include retry and failure handling.
   - Failed activities are logged for troubleshooting.

## Incremental Loading

Incremental loading can be implemented using a watermark column such as:

- last_modified_date
- created_date
- updated_timestamp

Only new or modified records are processed during incremental runs.

## Monitoring

Azure Data Factory Monitor is used to track:

- Pipeline execution status
- Activity status
- Execution duration
- Errors and failures
- Retry attempts

Alerts can be configured for pipeline failures.

## Architecture

The solution follows a Medallion Architecture:

### Bronze

Raw data copied from source systems.

### Silver

Cleaned and transformed data.

### Gold

Business-ready data optimized for analytics and reporting.

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Delta Lake
- Azure Synapse Analytics
- Power BI
