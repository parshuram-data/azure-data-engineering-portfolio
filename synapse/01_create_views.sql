-- Azure Synapse Analytics
-- Analytical serving views for the Azure Retail Data Platform
--
-- These views represent the serving layer over the curated
-- Gold datasets produced by the PySpark transformation pipeline.


-- =========================================================
-- Customer Sales View
-- =========================================================

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
-- Product Sales View
-- =========================================================

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
-- Customer Sales by State
-- =========================================================

CREATE VIEW vw_sales_by_state
AS
SELECT
    state,
    SUM(total_orders) AS total_orders,
    SUM(total_sales) AS total_sales
FROM gold_customer_sales
GROUP BY state;


-- =========================================================
-- Product Category Sales
-- =========================================================

CREATE VIEW vw_sales_by_category
AS
SELECT
    category,
    SUM(units_sold) AS units_sold,
    SUM(revenue) AS total_revenue
FROM gold_product_sales
GROUP BY category;
