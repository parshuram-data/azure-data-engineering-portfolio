-- =========================================================
-- Azure Synapse Analytics
-- Analytical Serving Layer
-- Azure Retail Data Platform
-- =========================================================
--
-- These views represent the analytical serving layer over
-- curated Gold datasets produced by the PySpark pipeline.
--
-- Gold datasets:
--   gold_customer_sales
--   gold_product_sales
--
-- The Gold layer contains analytics-ready Delta datasets.
-- In a deployed Azure environment, these datasets would be
-- exposed to Synapse through the appropriate external table,
-- serverless SQL, or ingestion mechanism.
-- =========================================================


-- =========================================================
-- 1. Customer Sales View
-- =========================================================
-- Provides customer-level sales metrics for reporting.

CREATE VIEW vw_customer_sales
AS
SELECT
    customer_id,
    customer_name,
    city,
    state,
    total_orders,
    total_sales
FROM gold_customer_sales;


-- =========================================================
-- 2. Product Sales View
-- =========================================================
-- Provides product-level sales metrics for reporting.

CREATE VIEW vw_product_sales
AS
SELECT
    product_id,
    product_name,
    category,
    units_sold,
    revenue
FROM gold_product_sales;


-- =========================================================
-- 3. Sales by State
-- =========================================================
-- Aggregates customer sales at state level.

CREATE VIEW vw_sales_by_state
AS
SELECT
    state,
    SUM(total_orders) AS total_orders,
    SUM(total_sales) AS total_sales
FROM gold_customer_sales
GROUP BY
    state;


-- =========================================================
-- 4. Sales by Product Category
-- =========================================================
-- Aggregates product sales by category.

CREATE VIEW vw_sales_by_category
AS
SELECT
    category,
    SUM(units_sold) AS units_sold,
    SUM(revenue) AS total_revenue
FROM gold_product_sales
GROUP BY
    category;
