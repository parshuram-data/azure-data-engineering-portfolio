# Power BI Reporting

This folder documents the reporting and visualization layer of the Azure Retail Data Platform.

Power BI consumes curated analytical data exposed through the Azure Synapse serving layer and provides business-facing dashboards and reports.

## Reporting Architecture

```text
Source Systems
      |
      v
Azure Data Factory
      |
      v
ADLS Gen2 Bronze
      |
      v
Azure Databricks / PySpark
      |
      v
ADLS Gen2 Silver Delta
      |
      v
Data Quality Checks
      |
      v
ADLS Gen2 Gold Delta
      |
      v
Azure Synapse Analytics
      |
      v
Power BI
