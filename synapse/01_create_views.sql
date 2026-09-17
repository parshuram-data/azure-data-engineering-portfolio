-- Azure Synapse Analytics
-- Analytical views for the Azure Retail Data Platform

-- Customer sales summary
CREATE VIEW vw_customer_sales
AS
SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_sales
FROM fact_orders
GROUP BY customer_id;


-- Product sales summary
CREATE VIEW vw_product_sales
AS
SELECT
    product_id,
    SUM(quantity) AS total_quantity,
    SUM(order_amount) AS total_sales
FROM fact_orders
GROUP BY product_id;


-- Daily sales summary
CREATE VIEW vw_daily_sales
AS
SELECT
    order_date,
    COUNT(order_id) AS total_orders,
    SUM(order_amount) AS total_sales
FROM fact_orders
GROUP BY order_date;
