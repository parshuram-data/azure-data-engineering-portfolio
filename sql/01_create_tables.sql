-- Azure Retail Data Platform
-- Create raw/staging tables

CREATE TABLE customers (
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date DATE
);

CREATE TABLE products (
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(12,2)
);

CREATE TABLE orders (
    order_id VARCHAR(20),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(12,2),
    status VARCHAR(30)
);

CREATE TABLE payments (
    payment_id VARCHAR(20),
    order_id VARCHAR(20),
    payment_date DATE,
    payment_method VARCHAR(50),
    amount DECIMAL(12,2),
    payment_status VARCHAR(30)
);
