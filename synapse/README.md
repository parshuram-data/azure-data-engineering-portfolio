# Azure Synapse Analytics

This folder documents the serving and analytics layer of the Azure Retail Data Platform.

## Purpose

Azure Synapse Analytics is used as the analytical serving layer for curated data produced by the data engineering pipeline.

## Data Flow

```text
Source Systems
      |
      v
Azure Data Factory
      |
      v
ADLS Gen2
      |
      v
Azure Databricks / PySpark
      |
      v
Delta Lake
      |
      v
Azure Synapse Analytics
      |
      v
Power BI
