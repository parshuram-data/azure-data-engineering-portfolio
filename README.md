
# Azure Retail Data Engineering Portfolio

An end-to-end Azure Data Engineering project demonstrating a modern cloud data platform for retail analytics.

## Project Overview

This project demonstrates how retail data can be ingested, transformed, validated, stored, and served for analytics using Microsoft Azure services.

The solution follows a Medallion Architecture:

**Bronze → Silver → Gold → Analytics → Reporting**

## Architecture

```text
Source Systems
     |
     v
CSV / Retail Data
     |
     v
Azure Data Factory
     |
     v
ADLS Gen2 - Bronze
     |
     v
Azure Databricks + PySpark
     |
     v
ADLS Gen2 - Silver
     |
     v
Databricks Transformations
     |
     v
Gold / Curated Data
     |
     v
Azure Synapse Analytics
     |
     v
Power BI
````

## Azure Services Used

* Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure Databricks
* PySpark
* Delta Lake
* Azure Synapse Analytics
* Power BI
* Azure DevOps
* GitHub Actions

## Data Sources

The project contains sample retail datasets:

* Customers
* Orders
* Products
* Payments

Sample data is stored under the `data/` directory.

## Medallion Architecture

### Bronze Layer

Stores raw source data with minimal transformation.

**Purpose:**

* Preserve source data
* Maintain historical raw data
* Support recovery and reprocessing

### Silver Layer

Cleans and standardizes the raw data using PySpark.

**Processing includes:**

* Data cleansing
* Duplicate removal
* Data type standardization
* Null handling
* Business rules

### Gold Layer

Contains curated data prepared for analytics and reporting.

**Used for:**

* Business reporting
* Aggregations
* Analytical queries
* Power BI dashboards

## Incremental Loading

The solution demonstrates incremental data loading using a watermark-based approach.

Instead of processing the complete dataset every time, only newly created or modified records are processed.

This helps reduce:

* Processing time
* Compute usage
* Data movement
* Pipeline execution cost

Implementation:

`adf/incremental_load.md`

## Data Quality

Data quality checks are implemented using PySpark.

Checks include:

* Null validation
* Duplicate detection
* Required column validation
* Basic data consistency checks

Implementation:

`pyspark/data_quality_checks.py`

## SQL Transformations

SQL scripts are maintained under the `sql/` directory.

They demonstrate:

* Table creation
* Data transformations
* Analytical queries
* Aggregations
* Joins
* Window functions

## Azure Synapse Analytics

Azure Synapse Analytics is used as the analytical serving layer.

The project includes analytical views designed to provide business-ready datasets for reporting.

Implementation:

`synapse/01_create_views.sql`

## Power BI

Power BI is used as the reporting and visualization layer.

It consumes curated analytical data from Azure Synapse to support business reporting and dashboard development.

Documentation:

`powerbi/README.md`

## CI/CD

The project demonstrates CI/CD practices using:

* Azure DevOps
* GitHub Actions

The GitHub Actions workflow automatically validates the PySpark source files when changes are pushed to the `main` branch.

Workflow:

`.github/workflows/ci.yml`

## Project Structure

```text
azure-data-engineering-portfolio/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── adf/
│   ├── README.md
│   ├── incremental_load.md
│   └── pipeline_retail_data.json
│
├── architecture/
│   ├── README.md
│   └── architecture.md
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── products.csv
│   ├── payments.csv
│   └── data_dictionary.md
│
├── devops/
│   ├── README.md
│   └── azure-pipelines.yml
│
├── powerbi/
│   └── README.md
│
├── pyspark/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── data_quality_checks.py
│
├── sql/
│   ├── 01_create_tables.sql
│   ├── 02_transformations.sql
│   └── 03_analytics.sql
│
├── synapse/
│   ├── README.md
│   └── 01_create_views.sql
│
└── README.md
```

## Key Data Engineering Concepts Demonstrated

* ETL / ELT pipelines
* Incremental data loading
* Medallion architecture
* Data lake architecture
* PySpark transformations
* Delta Lake
* Data quality validation
* SQL analytics
* Data warehousing
* Analytical views
* CI/CD
* Azure DevOps
* GitHub Actions

## End-to-End Data Flow

```text
Retail Data
    ↓
Azure Data Factory
    ↓
ADLS Gen2
    ↓
Bronze
    ↓
Databricks / PySpark
    ↓
Data Quality Checks
    ↓
Silver
    ↓
Business Transformations
    ↓
Gold
    ↓
Azure Synapse
    ↓
Power BI
```

## Objective

The objective of this portfolio project is to demonstrate practical Azure Data Engineering skills by building a complete data pipeline from ingestion to analytics and reporting.

```
```
