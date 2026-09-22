# Data Dictionary

This document describes the datasets used in the Azure Retail Data Platform.

## Customers

| Column | Description |
|---|---|
| customer_id | Unique identifier for each customer |
| customer_name | Customer name |
| city | Customer city |
| state | Customer state |
| signup_date | Date when the customer registered |

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
| unit_price | Price per unit at the time of the order |
| status | Current status of the order |

### Payments

| Column | Description |
|---|---|
| payment_id | Unique identifier for each payment |
| order_id | Related order identifier |
| payment_date | Date of payment |
| payment_method | Payment method used |
| amount | Payment amount |
| payment_status | Status of the payment |

### Derived Order Value

Order value is calculated during transformation:

```text
order_value = quantity × unit_price


