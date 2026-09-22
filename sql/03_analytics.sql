-- Azure Retail Data Platform
-- Analytics queries
-- Designed for Azure Synapse / T-SQL compatible analytics


-- 1. Top customers by total spending

SELECT
    c.customer_id,
    c.customer_name,
    SUM(o.quantity * o.unit_price) AS total_spending,
    COUNT(o.order_id) AS total_orders
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    c.customer_id,
    c.customer_name
ORDER BY total_spending DESC;


-- 2. Top-selling products

SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(o.quantity) AS units_sold,
    SUM(o.quantity * o.unit_price) AS revenue
FROM products p
JOIN orders o
    ON p.product_id = o.product_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    p.product_id,
    p.product_name,
    p.category
ORDER BY revenue DESC;


-- 3. Sales by city

SELECT
    c.city,
    COUNT(o.order_id) AS total_orders,
    SUM(o.quantity * o.unit_price) AS total_sales
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY
    c.city
ORDER BY total_sales DESC;


-- 4. Payment method analysis

SELECT
    payment_method,
    COUNT(payment_id) AS transaction_count,
    SUM(amount) AS total_amount
FROM payments
WHERE UPPER(TRIM(payment_status)) = 'PAID'
GROUP BY payment_method
ORDER BY total_amount DESC;


-- 5. Monthly sales by year

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


-- 6. Average order value

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


-- 7. Sales by product category

SELECT
    p.category,
    SUM(o.quantity) AS units_sold,
    SUM(o.quantity * o.unit_price) AS category_revenue
FROM products p
JOIN orders o
    ON p.product_id = o.product_id
WHERE UPPER(TRIM(o.status)) = 'COMPLETED'
GROUP BY p.category
ORDER BY category_revenue DESC;
