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
    UPPER(TRIM(status)) AS status
FROM orders;


-- 2. Calculate total completed sales
SELECT
    SUM(quantity * unit_price) AS total_sales
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED';


-- 3. Customer-wise sales
SELECT
    customer_id,
    SUM(quantity * unit_price) AS total_sales,
    COUNT(order_id) AS total_orders
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED'
GROUP BY customer_id;


-- 4. Product-wise sales
SELECT
    product_id,
    SUM(quantity * unit_price) AS total_sales,
    SUM(quantity) AS units_sold
FROM orders
WHERE UPPER(TRIM(status)) = 'COMPLETED'
GROUP BY product_id;


-- 5. Join customers with completed orders
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
