# Data Dictionary

This document describes the datasets used in the Azure Retail Data Platform.

## Customers

| Column | Description |
|---|---|
| customer_id | Unique identifier for each customer |
| customer_name | Customer name |
| email | Customer email address |
| city | Customer city |
| country | Customer country |

## Products

| Column | Description |
|---|---|
| product_id | Unique identifier for each product |
| product_name | Name of the product |
| category | Product category |
| price | Product selling price |

## Orders

| Column | Description |
|---|---|
| order_id | Unique identifier for each order |
| customer_id | Customer who placed the order |
| product_id | Product included in the order |
| order_date | Date when the order was placed |
| quantity | Quantity ordered |
| order_amount | Total order amount |

## Payments

| Column | Description |
|---|---|
| payment_id | Unique identifier for each payment |
| order_id | Related order identifier |
| payment_date | Date of payment |
| payment_method | Payment method used |
| payment_status | Status of the payment |
| amount | Payment amount |
