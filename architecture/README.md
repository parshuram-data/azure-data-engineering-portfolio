
## Architecture Diagram

The following diagram illustrates the end-to-end Azure Retail Data Engineering architecture.

![Azure Retail Data Engineering Architecture](Architecture.png)

### End-to-End Flow

```text
Data Sources
     ↓
Azure Data Factory
     ↓
ADLS Gen2 - Bronze
     ↓
Azure Databricks + PySpark
     ↓
ADLS Gen2 - Silver
     ↓
Databricks Transformations
     ↓
Gold / Curated Data
     ↓
Azure Synapse Analytics
     ↓
Power BI
````

### Key Components

* **Azure Data Factory** — Data ingestion and orchestration
* **ADLS Gen2** — Data lake storage
* **Azure Databricks** — PySpark-based data processing
* **Bronze Layer** — Raw source data
* **Silver Layer** — Cleaned and validated data
* **Gold Layer** — Curated analytical data
* **Azure Synapse Analytics** — Analytical serving layer
* **Power BI** — Reporting and visualization
* **Azure DevOps / GitHub Actions** — CI/CD and automation

````

