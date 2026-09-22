-- =========================================================
-- Azure Retail Data Platform
-- SQL Table Definitions
-- =========================================================
-- Purpose:
-- Defines the source-aligned tables used for SQL
-- transformations and analytical processing.
--
-- Source datasets:
--   customers.csv
--   products.csv
--   orders.csv
--   payments.csv
--
-- Note:
-- order_value is a derived metric:
-- quantity * unit_price
-- It is calculated during transformation rather than
-- stored in the source-aligned orders table.
-- =========================================================


-- =========================================================
-- Customers
-- =========================================================

CREATE TABLE customers (
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date DATE
);


-- =========================================================
-- Products
-- =========================================================

CREATE TABLE products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(12,2)
);


-- =========================================================
-- Orders
-- =========================================================

CREATE TABLE orders (
    order_id VARCHAR(20),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(12,2),
    status VARCHAR(30)
);


-- =========================================================
-- Payments
-- =========================================================

CREATE TABLE payments (
    payment_id VARCHAR(20),
    order_id VARCHAR(20),
    payment_date DATE,
    payment_method VARCHAR(50),
    amount DECIMAL(12,2),
    payment_status VARCHAR(30)
);
