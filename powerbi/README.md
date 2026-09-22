
# Power BI Reporting

This folder documents the reporting and visualization layer of the Azure Retail Data Platform.

Power BI consumes curated analytical data exposed through the Azure Synapse serving layer and provides business-facing dashboards and reports.

---

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
````

---

## Power BI Data Sources

Power BI is designed to consume analytical views exposed through the Synapse serving layer.

### Customer Sales

Synapse view:

```text
vw_customer_sales
```

Key fields:

| Field         | Description                |
| ------------- | -------------------------- |
| customer_id   | Unique customer identifier |
| customer_name | Customer name              |
| city          | Customer city              |
| state         | Customer state             |
| total_orders  | Number of completed orders |
| total_sales   | Total completed sales      |

---

### Product Sales

Synapse view:

```text
vw_product_sales
```

Key fields:

| Field        | Description                         |
| ------------ | ----------------------------------- |
| product_id   | Unique product identifier           |
| product_name | Product name                        |
| category     | Product category                    |
| units_sold   | Total units sold                    |
| revenue      | Total revenue from completed orders |

---

### Sales by State

Synapse view:

```text
vw_sales_by_state
```

Example reporting fields:

```text
state
total_orders
total_sales
```

This dataset supports geographic sales analysis.

---

### Sales by Product Category

Synapse view:

```text
vw_sales_by_category
```

Example reporting fields:

```text
category
units_sold
total_revenue
```

This dataset supports category-level performance analysis.

---

## Example Dashboard Areas

The analytical model supports several business-facing reporting areas.

### Executive Overview

Potential metrics:

* Total Sales
* Total Orders
* Average Order Value
* Units Sold
* Sales by State
* Sales by Product Category

### Customer Analysis

Potential visuals:

* Customer sales
* Orders per customer
* Sales by city
* Sales by state
* Customer-level performance

### Product Analysis

Potential visuals:

* Product revenue
* Units sold
* Product category performance
* Top-performing products
* Category-level revenue

### Geographic Analysis

Potential visuals:

* Sales by state
* Sales by city
* Order volume by location

---

## Example Business Measures

The reporting layer can calculate measures such as:

```text
Total Sales
= SUM(total_sales)
```

```text
Total Orders
= SUM(total_orders)
```

```text
Units Sold
= SUM(units_sold)
```

```text
Average Order Value
= Total Sales / Total Orders
```

These measures are based on completed-order sales represented in the Gold layer.

---

## Reporting Flow

The reporting flow follows the layered architecture:

```text
Raw Source Data
      |
      v
ADF Ingestion
      |
      v
Bronze
      |
      v
Silver Delta
      |
      v
Data Quality
      |
      v
Gold Delta
      |
      v
Synapse Analytical Views
      |
      v
Power BI
```

This separates data engineering and business reporting responsibilities.

---

## Performance Considerations

The reporting architecture is designed to minimize unnecessary processing in Power BI.

Key principles include:

* Perform major transformations in Databricks.
* Store curated analytical datasets in the Gold layer.
* Expose business-ready views through Synapse.
* Avoid loading unnecessary raw data into Power BI.
* Use aggregated Gold datasets for dashboard reporting.
* Keep Power BI calculations focused on business measures.

---

## Data Quality

Power BI consumes data after the Silver-layer data quality checks.

The pipeline validates:

* Duplicate records
* NULL identifiers
* NULL required fields
* Invalid quantities
* Negative prices
* Negative payment amounts
* NULL dates
* Other dataset-specific validation rules

This helps ensure that reporting is based on validated analytical data.

---

## Security Considerations

In a production Azure implementation, Power BI access can be controlled through:

* Microsoft Entra ID
* Azure role-based access control
* Power BI workspace permissions
* Row-level security where required
* Secure Synapse connectivity

Security configuration is environment-specific and is not included as a live deployment in this portfolio repository.

---

## Repository Structure

```text
powerbi/
└── README.md
```

This folder currently documents the Power BI reporting architecture and semantic/reporting design.

The repository does not claim to contain a deployed Power BI `.pbix` file.

---

## Technologies

* Power BI
* Azure Synapse Analytics
* Azure Databricks
* PySpark
* Delta Lake
* Azure Data Lake Storage Gen2
* Azure Data Factory

````

