-- =========================================================
-- Azure Retail Data Platform
-- SQL Business Transformations
-- =========================================================
-- Purpose:
-- Applies business transformations to the source-aligned
-- SQL tables.
--
-- Key business rules:
--   1. Standardize order status to uppercase.
--   2. Calculate order_value as quantity * unit_price.
--   3. Include only COMPLETED orders in sales metrics.
--   4. Aggregate sales by customer and product.
-- =========================================================


-- =========================================================
-- 1. Calculate Order Value
-- =========================================================
-- Derives order_value from quantity and unit price.

SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    quantity * unit_price AS order_value,
    UPPER(TRIM(status)) AS status
FROM orders;


-- =========================================================
-- 2. Calculate Total Completed Sales
-- =========================================================
-- Cancelled orders are excluded from revenue calculations.

SELECT
    SUM(quantity * unit_price) AS total_sales
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED';


-- =========================================================
-- 3. Customer-Wise Sales
-- =========================================================
-- Calculates total revenue and number of completed orders
-- for each customer.

SELECT
    customer_id,
    SUM(quantity * unit_price) AS total_sales,
    COUNT(order_id) AS total_orders
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED'
GROUP BY customer_id;


-- =========================================================
-- 4. Product-Wise Sales
-- =========================================================
-- Calculates units sold and revenue for each product.

SELECT
    product_id,
    SUM(quantity) AS units_sold,
    SUM(quantity * unit_price) AS revenue
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED'
GROUP BY product_id;


-- =========================================================
-- 5. Customer Order Details
-- =========================================================
-- Joins customers with their completed orders.

SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    o.order_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_value
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED';
