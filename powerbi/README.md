# Power BI Reporting

This folder documents the reporting and visualization layer of the Azure Retail Data Platform.

## Purpose

Power BI consumes curated analytical data from Azure Synapse Analytics to provide business dashboards and reports.

## Data Flow

```text
Azure Data Lake Gen2
        |
        v
Azure Databricks
        |
        v
Delta Lake Gold Layer
        |
        v
Azure Synapse Analytics
        |
        v
Power BI
