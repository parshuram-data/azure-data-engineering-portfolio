# Azure Synapse Analytics

This folder documents the analytical serving layer of the Azure Retail Data Platform.

Azure Synapse Analytics is used to expose curated Gold data for downstream analytics and reporting.

## Data Flow

```text
Source Systems
      |
      v
Azure Data Factory
      |
      v
ADLS Gen2 - Bronze
      |
      v
Azure Databricks / PySpark
      |
      v
ADLS Gen2 - Silver Delta
      |
      v
Data Quality Checks
      |
      v
ADLS Gen2 - Gold Delta
      |
      v
Azure Synapse Analytics
      |
      v
Power BI
