
# Azure Data Engineering Portfolio

## Parashuram | Azure Data Engineer

A practical Azure Data Engineering portfolio demonstrating an end-to-end
cloud data platform using Azure Data Factory, Azure Data Lake Storage Gen2,
Azure Databricks, PySpark, Delta Lake, Azure Synapse and Power BI.

The project focuses on building a maintainable Medallion Architecture,
metadata-driven ingestion patterns, data quality validation, analytical
transformations and CI/CD validation.

---

## 🏗️ Architecture

```text
                         SOURCE DATA
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
    Customers             Products             Orders
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                         Payments
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Azure Data Factory   │
                  │                     │
                  │ Metadata-driven     │
                  │ orchestration       │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ ADLS Gen2           │
                  │                     │
                  │ Bronze              │
                  │ Raw source data     │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Azure Databricks    │
                  │ PySpark             │
                  │                     │
                  │ Cleaning            │
                  │ Standardization     │
                  │ Deduplication       │
                  │ Validation          │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Silver              │
                  │                     │
                  │ Curated Delta       │
                  │ datasets            │
                  └──────────┬──────────┘
                             │
                    Data Quality Checks
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Gold                │
                  │                     │
                  │ Customer Sales      │
                  │ Product Sales       │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Azure Synapse       │
                  │                     │
                  │ Analytical views    │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Power BI            │
                  │                     │
                  │ Reporting &         │
                  │ analytics           │
                  └─────────────────────┘
````

---

# 🎯 Project Objective

The objective of this portfolio project is to demonstrate how a modern
Azure data platform can ingest source data, apply data engineering
transformations, validate data quality and expose analytics-ready datasets
for reporting.

The implementation demonstrates:

* Azure Data Factory orchestration
* Metadata-driven ingestion patterns
* ADLS Gen2 Medallion Architecture
* PySpark transformations
* Delta Lake datasets
* Data quality validation
* Analytical Gold datasets
* SQL analytics
* Azure Synapse serving-layer patterns
* Power BI reporting architecture
* GitHub Actions CI validation
* Azure DevOps CI validation

---

# 📂 Source Data

The project uses representative CSV datasets.

### Customers

```text
customer_id
customer_name
city
state
signup_date
```

### Products

```text
product_id
product_name
category
price
```

### Orders

```text
order_id
customer_id
product_id
order_date
quantity
unit_price
status
```

### Payments

```text
payment_id
order_id
payment_date
payment_method
amount
payment_status
```

The source data is intentionally small so that the complete transformation
logic can be reviewed and reproduced easily.

---

# 🔄 Data Engineering Flow

## 1. Ingestion

Azure Data Factory is used as the orchestration layer.

The repository contains a metadata configuration that describes:

* Source dataset
* Source type
* Source path
* Target path
* Load type
* Watermark column
* Target layer
* Active status

Example:

```text
customers → Incremental → signup_date
orders    → Incremental → order_date
products  → Full
payments  → Incremental → payment_date
```

The metadata-driven approach provides a reusable orchestration pattern
instead of creating completely independent pipelines for every source.

---

# 🥉 Bronze Layer

The Bronze layer represents the raw ingestion layer.

Source datasets are organized as:

```text
bronze/
├── customers/
├── products/
├── orders/
└── payments/
```

The Bronze layer is intended to preserve source data before business
transformations are applied.

---

# 🥈 Silver Layer

Azure Databricks and PySpark are used to create curated Delta datasets.

Transformations include:

* Column trimming
* Date conversion
* Numeric type conversion
* Status standardization
* Category standardization
* Positive quantity validation
* Non-negative price validation
* Duplicate removal

Silver datasets:

```text
silver/
├── customers/
├── products/
├── orders/
└── payments/
```

The Silver layer provides cleaner and standardized datasets for downstream
analytics.

---

# ✅ Data Quality

The project includes a dedicated PySpark data-quality validation process.

The checks cover:

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

The pipeline produces a final:

```text
DATA QUALITY STATUS: PASSED
```

or

```text
DATA QUALITY STATUS: FAILED
```

result based on the validation checks.

---

# 🥇 Gold Layer

The Gold layer contains analytics-ready Delta datasets.

## Customer Sales

The customer sales dataset contains:

```text
customer_id
customer_name
city
state
total_orders
total_sales
```

## Product Sales

The product sales dataset contains:

```text
product_id
product_name
category
units_sold
revenue
```

Sales calculations use:

```text
order_value = quantity × unit_price
```

Only orders with:

```text
status = COMPLETED
```

are included in sales calculations.

Cancelled orders are therefore excluded from revenue and order-sales
aggregations.

---

# 🧮 SQL Analytics

The SQL layer contains analytical queries for:

* Customer sales
* Product sales
* Sales by city
* Sales by state
* Payment-method analysis
* Monthly sales
* Average order value
* Category-level sales

SQL transformations follow the same business rules used by the PySpark
Gold layer.

This provides an additional SQL-based analytical representation of the
data platform.

---

# 🗄️ Azure Synapse

Azure Synapse is represented as the analytical serving layer.

The repository contains view definitions for:

```text
vw_customer_sales
vw_product_sales
vw_sales_by_state
vw_sales_by_category
```

The intended reporting flow is:

```text
Gold Delta datasets
        ↓
Synapse analytical views
        ↓
Power BI
```

The repository contains the SQL serving-layer definitions rather than a
live deployed Synapse workspace.

---

# 📊 Power BI

Power BI is represented as the reporting and visualization layer.

The documented reporting sources are:

```text
vw_customer_sales
vw_product_sales
vw_sales_by_state
vw_sales_by_category
```

Potential dashboard areas include:

* Total sales
* Total orders
* Customer sales
* Product performance
* State-level sales
* Category performance
* Monthly sales trends
* Average order value

The repository documents the reporting architecture; a deployed `.pbix`
file is not included.

---

# 🔁 Incremental Loading

The ADF metadata configuration includes an incremental-loading design using
configurable watermark columns.

Example:

```text
customers → signup_date
orders    → order_date
payments  → payment_date
```

The repository documents the intended pattern:

```text
Read watermark
      ↓
Identify new/changed records
      ↓
Ingest records
      ↓
Update watermark
```

The current sample implementation focuses on the orchestration and metadata
pattern. A persistent production watermark store and complete dynamic
source filtering mechanism are not included.

This keeps the portfolio explicit about what is implemented versus what is
represented as an architectural pattern.

---

# ⚙️ CI/CD

The repository contains CI validation using both GitHub Actions and Azure
DevOps.

## GitHub Actions

The workflow validates:

* PySpark syntax
* ADF pipeline JSON
* Required project files
* SQL files
* Metadata configuration

## Azure DevOps

The Azure Pipeline validates:

* Python environment
* PySpark source files
* ADF pipeline JSON
* Required project structure
* SQL files
* Metadata configuration

These pipelines are validation pipelines. They do not claim to deploy
live Azure resources.

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
│   ├── pipeline_retail_data.json
│   ├── README.md
│   └── incremental_load.md
│
├── architecture/
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── payments.csv
│   └── data_dictionary.md
│
├── devops/
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

# 🛠️ Technologies

| Technology                   | Purpose                     |
| ---------------------------- | --------------------------- |
| Azure Data Factory           | Data orchestration          |
| Azure Data Lake Storage Gen2 | Data lake storage           |
| Azure Databricks             | Data transformation         |
| PySpark                      | Distributed data processing |
| Delta Lake                   | Curated data storage        |
| Azure Synapse                | Analytical serving layer    |
| Power BI                     | Reporting and visualization |
| SQL                          | Data modeling and analytics |
| GitHub Actions               | CI validation               |
| Azure DevOps                 | CI validation               |
| Git                          | Version control             |

---

# 🧠 Engineering Concepts Demonstrated

This portfolio demonstrates practical knowledge of:

* Medallion Architecture
* Metadata-driven pipeline design
* ETL / ELT patterns
* Incremental-loading design
* Data cleansing
* Data standardization
* Deduplication
* Data quality validation
* Delta Lake
* Analytical aggregations
* SQL transformations
* Serving-layer design
* CI/CD validation
* Parameterized Databricks notebooks
* Separation of Bronze, Silver and Gold responsibilities

---

# 📈 Performance Engineering Concepts

The repository is structured to support common Azure data-engineering
performance techniques such as:

* Partition-aware processing
* Predicate filtering
* Broadcast joins for suitable lookup datasets
* Spark execution-plan analysis
* Delta Lake optimization
* Appropriate file sizing
* Avoiding unnecessary data scans

These are documented engineering techniques rather than claims of a
measured production benchmark in this sample repository.

---

# 🔐 Data Engineering Design Principles

The project follows several practical design principles:

### Separation of layers

Raw, curated and analytical datasets are separated into Bronze, Silver and
Gold layers.

### Reusable orchestration

Metadata is used to describe source datasets and ingestion behavior.

### Data quality before analytics

Silver datasets are validated before downstream Gold transformations.

### Consistent business logic

The PySpark and SQL layers apply the same completed-order revenue rule.

### Transparent implementation

The repository distinguishes between implemented code and architectural
patterns that would require additional Azure resources in production.

---

# 🚀 Future Enhancements

Potential production extensions include:

* Persistent watermark control tables
* Fully dynamic ADF datasets and linked services
* Parameterized source and sink datasets
* Azure Key Vault integration
* Azure DevOps deployment stages
* Databricks job deployment
* Automated testing
* Schema-drift detection
* Advanced data-quality reporting
* Delta Lake MERGE-based incremental processing
* Production monitoring and alerting
* Synapse workspace deployment
* Power BI `.pbix` implementation
* Infrastructure as Code using Bicep or Terraform

---

# 👨‍💻 About

**Parashuram**

Azure Data Engineer with experience in building data pipelines and
analytics solutions using the Azure data platform.

### Core Skills

```text
Azure Data Factory
Azure Databricks
PySpark
Azure Data Lake Storage Gen2
Azure Synapse
Delta Lake
SQL
Python
Git
GitHub Actions
Azure DevOps
Power BI
```

---

## 📌 Portfolio Disclaimer

This repository is a technical portfolio project designed to demonstrate
Azure Data Engineering concepts and implementation patterns.

Some components, including the Synapse serving layer, Power BI reporting
layer and production-grade incremental-loading infrastructure, are
represented through configuration, SQL and documentation rather than live
deployed Azure resources.

The sample data is synthetic and does not represent production customer
data.

