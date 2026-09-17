-- Azure Retail Data Platform
-- Business transformation queries

-- 1. Calculate order value
SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    quantity * unit_price AS order_value,
    status
FROM orders;


-- 2. Calculate total completed sales
SELECT
    SUM(quantity * unit_price) AS total_sales
FROM orders
WHERE status = 'Completed';


-- 3. Customer-wise sales
SELECT
    customer_id,
    SUM(quantity * unit_price) AS total_sales,
    COUNT(order_id) AS total_orders
FROM orders
WHERE status = 'Completed'
GROUP BY customer_id;


-- 4. Product-wise sales
SELECT
    product_id,
    SUM(quantity * unit_price) AS total_sales,
    SUM(quantity) AS units_sold
FROM orders
WHERE status = 'Completed'
GROUP BY product_id;


-- 5. Join customers with orders
SELECT
    c.customer_id,
    c.customer_name,
    c.city,
    o.order_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_value
FROM customers c
INNER JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.status = 'Completed';
