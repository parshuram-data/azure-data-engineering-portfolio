
# 🚀 Azure Retail Data Engineering Portfolio

An end-to-end Azure Data Engineering portfolio demonstrating a retail data platform using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, Azure Synapse Analytics, SQL and Power BI.

The project follows a Medallion Architecture:

```text
Source Data
    ↓
Azure Data Factory
    ↓
ADLS Gen2 Bronze
    ↓
Azure Databricks / PySpark
    ↓
ADLS Gen2 Silver
    ↓
Data Quality Checks
    ↓
ADLS Gen2 Gold
    ↓
Azure Synapse Analytics
    ↓
Power BI
````

---

# 🏗️ Architecture

## End-to-End Data Flow

```text
Retail CSV Source Data
        │
        ▼
Azure Data Factory
        │
        ▼
ADLS Gen2
Bronze Layer
        │
        ▼
Azure Databricks / PySpark
        │
        ▼
ADLS Gen2
Silver Delta Layer
        │
        ▼
Data Quality Checks
        │
        ▼
ADLS Gen2
Gold Delta Layer
        │
        ▼
Azure Synapse Analytics
        │
        ▼
Power BI
```

The repository contains representative source datasets for:

* Customers
* Products
* Orders
* Payments

---

# 📌 Project Overview

This project demonstrates how retail data can be ingested, transformed, validated, aggregated and exposed for analytical reporting using Azure data engineering technologies.

The implementation demonstrates:

* Batch data ingestion
* Metadata-driven pipeline design
* Medallion Architecture
* PySpark transformations
* Delta Lake storage
* Data cleansing
* Deduplication
* Data quality validation
* Business aggregations
* SQL analytics
* Synapse analytical views
* Power BI reporting design
* CI validation
* Error-handling patterns

---

# 🥉 Bronze Layer

The Bronze layer represents the raw ingestion zone.

Source datasets:

```text
data/
├── customers.csv
├── products.csv
├── orders.csv
└── payments.csv
```

The ADF pipeline is designed to ingest these datasets into separate Bronze folders:

```text
bronze/
├── customers/
│   └── customers.csv
├── products/
│   └── products.csv
├── orders/
│   └── orders.csv
└── payments/
    └── payments.csv
```

The Bronze layer preserves the source structure before PySpark transformations are applied.

---

# 🥈 Silver Layer

The Silver layer contains cleaned and standardized Delta datasets.

PySpark transformations include:

* Trimming text fields
* Standardizing status values
* Converting dates
* Explicit numeric type casting
* Removing duplicate business keys
* Filtering invalid quantities
* Filtering invalid prices
* Filtering invalid payment amounts

Silver datasets:

```text
silver/
├── customers/
├── products/
├── orders/
└── payments/
```

### Example standardization

Order status values are standardized to uppercase:

```text
Completed → COMPLETED
Cancelled → CANCELLED
```

Payment status values are also standardized:

```text
Paid → PAID
Refunded → REFUNDED
```

---

# 🧹 Data Quality

The Silver layer is validated before Gold processing.

The data quality notebook checks:

### Customers

* Duplicate customer IDs
* NULL customer IDs
* NULL customer names
* NULL signup dates

### Products

* Duplicate product IDs
* NULL product IDs
* NULL product names
* NULL prices
* Negative prices

### Orders

* Duplicate order IDs
* NULL order IDs
* NULL customer IDs
* NULL product IDs
* NULL order dates
* NULL unit prices
* Invalid quantities
* Negative unit prices

### Payments

* Duplicate payment IDs
* NULL payment IDs
* NULL order IDs
* NULL payment dates
* NULL payment amounts
* Negative payment amounts

The notebook reports an overall:

```text
DATA QUALITY STATUS: PASSED
```

or

```text
DATA QUALITY STATUS: FAILED
```

---

# 🥇 Gold Layer

The Gold layer contains curated analytical datasets.

The current PySpark Gold transformation creates:

```text
gold/
├── customer_sales/
└── product_sales/
```

## Customer Sales

Contains:

* customer_id
* customer_name
* city
* state
* total_orders
* total_sales

## Product Sales

Contains:

* product_id
* product_name
* category
* units_sold
* revenue

### Revenue calculation

Order value is derived as:

```text
order_value = quantity × unit_price
```

Only orders with:

```text
status = COMPLETED
```

are included in sales calculations.

Cancelled orders are excluded from revenue reporting.

---

# 🔄 ADF Pipeline

Azure Data Factory is used as the orchestration layer.

The pipeline contains the following activities:

```text
Lookup_Source_Metadata
        ↓
ForEach_Source_Table
        ↓
Copy_To_Bronze
        ↓
Bronze_To_Silver_Databricks
        ↓
Data_Quality_Checks
        ↓
Silver_To_Gold_Databricks
```

A failure-handling activity is also included as a notification integration pattern.

## Metadata Configuration

The repository contains:

```text
adf/metadata/pipeline_config.csv
```

The configuration describes:

* Source name
* Source type
* Source path
* Bronze target path
* Load type
* Watermark column
* Target layer
* Active flag

Example:

```text
customers → data/customers.csv → bronze/customers
orders    → data/orders.csv    → bronze/orders
products  → data/products.csv  → bronze/products
payments  → data/payments.csv  → bronze/payments
```

The metadata-driven structure demonstrates how a reusable ingestion framework can be designed instead of creating a separate pipeline definition for every dataset.

---

# 🔄 Incremental Loading

The metadata configuration documents incremental-load fields such as:

```text
customers → signup_date
orders    → order_date
payments  → payment_date
```

The current repository demonstrates the **metadata and orchestration design** for incremental processing.

A production implementation would typically maintain a persistent watermark value and apply it during source extraction using a source-system modification timestamp or equivalent change-tracking mechanism.

---

# ⚡ Azure Databricks & PySpark

Databricks/PySpark is used for:

* Data cleansing
* Type standardization
* Deduplication
* Filtering invalid records
* Joins
* Aggregations
* Business-rule implementation
* Data quality validation
* Gold-layer generation

The main PySpark components are:

```text
pyspark/
├── bronze_to_silver.py
├── data_quality_checks.py
└── silver_to_gold.py
```

---

# 🗄️ Delta Lake

Delta Lake is used for Silver and Gold analytical storage.

The transformation notebooks write datasets using:

```python
.format("delta")
```

Delta provides capabilities such as:

* ACID transactions
* Schema enforcement
* Time travel
* Reliable data storage
* Transaction history
* Incremental data processing patterns

---

# 🧠 SQL Analytics

The SQL layer contains:

```text
sql/
├── 01_create_tables.sql
├── 02_transformations.sql
└── 03_analytics.sql
```

Analytical queries include:

* Customer sales analysis
* Product sales analysis
* Sales by city
* Sales by state
* Payment method analysis
* Monthly sales
* Average order value
* Sales by product category

---

# 🧠 Azure Synapse Analytics

Synapse represents the analytical serving layer.

The repository contains views for the curated Gold datasets:

```text
vw_customer_sales
vw_product_sales
vw_sales_by_state
vw_sales_by_category
```

These views provide a SQL-based serving layer for downstream reporting.

The repository documents the Synapse SQL layer; it does not claim that a live Synapse workspace or production deployment is included.

---

# 📊 Power BI

Power BI represents the reporting layer.

The reporting design supports:

* Sales performance
* Customer analysis
* Product performance
* Sales by state
* Sales by category
* Monthly sales trends
* Average order value

The repository currently contains the reporting architecture and semantic design rather than a deployed `.pbix` report.

---

# 🔁 CI/CD

The repository includes CI validation examples using:

* GitHub Actions
* Azure DevOps Pipelines

The GitHub Actions workflow validates the PySpark files using Python compilation.

The Azure DevOps pipeline validates the Python environment, PySpark files and project structure.

These workflows demonstrate CI validation practices.

They are not presented as live Azure resource deployment pipelines.

---

# 📁 Repository Structure

```text
azure-data-engineering-portfolio/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── adf/
│   ├── metadata/
│   │   └── pipeline_config.csv
│   ├── README.md
│   ├── incremental_load.md
│   └── pipeline_retail_data.json
│
├── architecture/
│   ├── Architecture.png
│   ├── README.md
│   └── architecture.md
│
├── data/
│   ├── customers.csv
│   ├── orders.csv
│   ├── payments.csv
│   ├── products.csv
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
│   ├── data_quality_checks.py
│   └── silver_to_gold.py
│
├── sql/
│   ├── 01_create_tables.sql
│   ├── 02_transformations.sql
│   └── 03_analytics.sql
│
├── synapse/
│   ├── 01_create_views.sql
│   └── README.md
│
└── README.md
```

---

# 🛠️ Technology Stack

| Technology                   | Purpose                           |
| ---------------------------- | --------------------------------- |
| Azure Data Factory           | Data ingestion and orchestration  |
| Azure Data Lake Storage Gen2 | Data lake storage                 |
| Azure Databricks             | Distributed data processing       |
| PySpark                      | Data transformation               |
| Delta Lake                   | Curated analytical storage        |
| Azure Synapse Analytics      | Analytical serving                |
| SQL                          | Data transformation and analytics |
| Power BI                     | Reporting and visualization       |
| GitHub Actions               | CI validation                     |
| Azure DevOps                 | CI pipeline example               |
| Python                       | Data engineering automation       |

---

# 🎯 Key Data Engineering Skills Demonstrated

* Azure Data Engineering
* ETL / ELT
* Azure Data Factory
* Azure Databricks
* PySpark
* ADLS Gen2
* Delta Lake
* Azure Synapse
* SQL
* Medallion Architecture
* Metadata-driven pipeline design
* Data Quality
* Data Modeling
* Incremental Processing Patterns
* CI/CD
* Git
* Power BI

---

# 📌 Project Highlights

### End-to-End Architecture

Demonstrates the flow from source ingestion through transformation, analytical serving and reporting.

### Medallion Architecture

```text
Bronze → Silver → Gold
```

provides controlled data refinement across the platform.

### Data Quality

Automated PySpark validation checks are applied before Gold processing.

### Reusable Pipeline Design

ADF metadata configuration demonstrates how multiple datasets can be handled through a reusable orchestration pattern.

### Analytical Modeling

Gold datasets provide curated customer and product sales metrics for downstream analytics.

### Production-Oriented Practices

The repository demonstrates patterns for:

* Parameterized pipelines
* Data quality
* Error handling
* CI validation
* Documentation
* Layered data architecture

---

# 👨‍💻 About

**Parshuram**

Azure Data Engineer specializing in:

Azure Data Factory | Azure Databricks | PySpark | ADLS Gen2 | Azure Synapse | Delta Lake | SQL | Data Engineering

---

⭐ Explore the repository to review the individual ADF, PySpark, SQL, Synapse, Power BI and DevOps components.

````

