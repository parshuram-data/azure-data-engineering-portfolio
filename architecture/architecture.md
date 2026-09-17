# Azure Retail Data Platform - Architecture

## Overview

This project demonstrates an end-to-end Azure data engineering solution for a simulated retail data platform.

The architecture uses Azure Data Factory for orchestration, ADLS Gen2 for data storage, Azure Databricks and PySpark for transformation, Delta Lake for reliable data processing, and Azure Synapse Analytics for analytics and reporting.

## End-to-End Architecture

```text
                    SOURCE SYSTEMS
                         |
        +----------------+----------------+
        |                |                |
     SQL Server       CSV Files        REST API
        |                |                |
        +----------------+----------------+
                         |
                         v
                +-------------------+
                | Azure Data Factory |
                |   Orchestration    |
                +---------+---------+
                          |
                          v
                +-------------------+
                |    ADLS Gen2      |
                |   Bronze Layer    |
                |   Raw Data        |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Azure Databricks  |
                |      PySpark      |
                | Transformation    |
                +---------+---------+
                          |
                          v
                +-------------------+
                |    Silver Layer   |
                | Cleaned Data      |
                | Deduplicated Data |
                +---------+---------+
                          |
                    Data Quality
                       Checks
                          |
                          v
                +-------------------+
                |     Gold Layer    |
                |   Delta Tables    |
                | Analytics Ready   |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Azure Synapse     |
                |    Analytics      |
                +---------+---------+
                          |
                          v
                +-------------------+
                |     Power BI      |
                | Reporting & BI    |
                +-------------------+
