-- =========================================================
-- Azure Retail Data Platform
-- Analytical Queries
-- =========================================================
-- Designed for Azure Synapse / T-SQL compatible analytics.
--
-- Business rules:
--   - Only COMPLETED orders are included in sales metrics.
--   - order_value = quantity * unit_price.
--   - Refunded payments are excluded from paid-payment analysis.
-- =========================================================


-- =========================================================
-- 1. Customer Sales Analysis
-- =========================================================
-- Calculates total spending and order count per customer.

SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    SUM(o.quantity * o.unit_price) AS total_sales,
    COUNT(o.order_id) AS total_orders
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    c.customer_id,
    c.customer_name,
    c.city,
    c.state
ORDER BY total_sales DESC;


-- =========================================================
-- 2. Product Sales Analysis
-- =========================================================
-- Calculates units sold and revenue per product.

SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(o.quantity) AS units_sold,
    SUM(o.quantity * o.unit_price) AS revenue
FROM products p
INNER JOIN orders o
    ON p.product_id = o.product_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY revenue DESC;


-- =========================================================
-- 3. Sales by City
-- =========================================================
-- Provides geographic sales analysis at city level.

SELECT
    c.city,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity * o.unit_price) AS total_sales
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    c.city
ORDER BY total_sales DESC;


-- =========================================================
-- 4. Sales by State
-- =========================================================
-- Provides geographic sales analysis at state level.

SELECT
    c.state,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity * o.unit_price) AS total_sales
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    c.state
ORDER BY total_sales DESC;


-- =========================================================
-- 5. Payment Method Analysis
-- =========================================================
-- Only PAID transactions are included.
-- Refunded payments are excluded.

SELECT
    UPPER(TRIM(payment_method)) AS payment_method,
    COUNT(payment_id) AS transaction_count,
    SUM(amount) AS total_amount
FROM payments
WHERE UPPER(TRIM(payment_status)) = 'PAID'
GROUP BY
    UPPER(TRIM(payment_method))
ORDER BY total_amount DESC;


-- =========================================================
-- 6. Monthly Sales
-- =========================================================
-- Calculates completed sales by year and month.

SELECT
    DATEPART(YEAR, order_date) AS order_year,
    DATEPART(MONTH, order_date) AS order_month,
    SUM(quantity * unit_price) AS total_sales,
    COUNT(order_id) AS total_orders
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED'
GROUP BY
    DATEPART(YEAR, order_date),
    DATEPART(MONTH, order_date)
ORDER BY
    order_year,
    order_month;


-- =========================================================
-- 7. Average Order Value
-- =========================================================
-- AOV = Total Sales / Number of Completed Orders.

SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity * unit_price) AS total_sales,
    CAST(
        SUM(quantity * unit_price) /
        NULLIF(COUNT(DISTINCT order_id), 0)
        AS DECIMAL(12,2)
    ) AS average_order_value
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED';


-- =========================================================
-- 8. Sales by Product Category
-- =========================================================
-- Calculates units sold and revenue by category.

SELECT
    p.category,
    SUM(o.quantity) AS units_sold,
    SUM(o.quantity * o.unit_price) AS category_revenue
FROM products p
INNER JOIN orders o
    ON p.product_id = o.product_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    p.category
ORDER BY category_revenue DESC;
