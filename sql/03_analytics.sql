-- Azure Retail Data Platform
-- Analytics queries

-- 1. Top customers by total spending
SELECT
    c.customer_id,
    c.customer_name,
    SUM(o.quantity * o.unit_price) AS total_spending
FROM customers c
JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.status = 'Completed'
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
WHERE o.status = 'Completed'
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
WHERE o.status = 'Completed'
GROUP BY c.city
ORDER BY total_sales DESC;


-- 4. Payment method analysis
SELECT
    payment_method,
    COUNT(payment_id) AS transaction_count,
    SUM(amount) AS total_amount
FROM payments
WHERE payment_status = 'Paid'
GROUP BY payment_method
ORDER BY total_amount DESC;


-- 5. Monthly sales
SELECT
    EXTRACT(MONTH FROM order_date) AS order_month,
    SUM(quantity * unit_price) AS total_sales
FROM orders
WHERE status = 'Completed'
GROUP BY EXTRACT(MONTH FROM order_date)
ORDER BY order_month;
