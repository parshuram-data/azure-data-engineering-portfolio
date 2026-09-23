from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round as spark_round
)

spark = SparkSession.builder.appName(
    "RetailReconciliationChecks"
).getOrCreate()

# =========================================================
# Parameters
# =========================================================

dbutils.widgets.text("silver_path", "silver")

silver_path = dbutils.widgets.get("silver_path").strip()

if not silver_path:
    raise ValueError("silver_path parameter cannot be empty")

print("=" * 80)
print("RETAIL DATA RECONCILIATION")
print("=" * 80)
print(f"Silver path: {silver_path}")

# =========================================================
# Result tracking
# =========================================================

reconciliation_results = []
critical_failures = []


def record_check(
    check_name,
    status,
    actual_value,
    expected_value,
    message
):
    result = {
        "check": check_name,
        "status": status,
        "actual": actual_value,
        "expected": expected_value,
        "message": message
    }

    reconciliation_results.append(result)

    if status == "PASS":
        print(
            f"[PASS] {check_name} | "
            f"Actual={actual_value} | "
            f"Expected={expected_value} | "
            f"{message}"
        )
    else:
        print(
            f"[FAIL] {check_name} | "
            f"Actual={actual_value} | "
            f"Expected={expected_value} | "
            f"{message}"
        )

        critical_failures.append(result)


def read_silver_table(table_name):
    path = f"{silver_path}/{table_name}"

    print(f"\nReading Silver table: {path}")

    try:
        return spark.read.format("delta").load(path)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to read Silver table '{table_name}': {str(exc)}"
        )


# =========================================================
# Read Silver tables
# =========================================================

customers_df = read_silver_table("customers")
products_df = read_silver_table("products")
orders_df = read_silver_table("orders")
payments_df = read_silver_table("payments")


# =========================================================
# 1. Customer reference integrity
# =========================================================

orphan_order_customers = (
    orders_df.alias("o")
    .join(
        customers_df.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left_anti"
    )
    .count()
)

record_check(
    check_name="ORDER_CUSTOMER_REFERENTIAL_INTEGRITY",
    status="PASS" if orphan_order_customers == 0 else "FAIL",
    actual_value=orphan_order_customers,
    expected_value=0,
    message=(
        "Every order references an existing customer"
        if orphan_order_customers == 0
        else f"{orphan_order_customers} orders reference missing customers"
    )
)


# =========================================================
# 2. Product reference integrity
# =========================================================

orphan_order_products = (
    orders_df.alias("o")
    .join(
        products_df.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left_anti"
    )
    .count()
)

record_check(
    check_name="ORDER_PRODUCT_REFERENTIAL_INTEGRITY",
    status="PASS" if orphan_order_products == 0 else "FAIL",
    actual_value=orphan_order_products,
    expected_value=0,
    message=(
        "Every order references an existing product"
        if orphan_order_products == 0
        else f"{orphan_order_products} orders reference missing products"
    )
)


# =========================================================
# 3. Payment → Order reference integrity
# =========================================================

orphan_payments = (
    payments_df.alias("p")
    .join(
        orders_df.alias("o"),
        col("p.order_id") == col("o.order_id"),
        "left_anti"
    )
    .count()
)

record_check(
    check_name="PAYMENT_ORDER_REFERENTIAL_INTEGRITY",
    status="PASS" if orphan_payments == 0 else "FAIL",
    actual_value=orphan_payments,
    expected_value=0,
    message=(
        "Every payment references an existing order"
        if orphan_payments == 0
        else f"{orphan_payments} payments reference missing orders"
    )
)


# =========================================================
# 4. Completed order ↔ paid payment relationship
# =========================================================

completed_orders_df = (
    orders_df
    .filter(col("status") == "COMPLETED")
)

paid_payments_df = (
    payments_df
    .filter(col("payment_status") == "PAID")
)

completed_without_payment = (
    completed_orders_df.alias("o")
    .join(
        paid_payments_df.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left_anti"
    )
    .count()
)

record_check(
    check_name="COMPLETED_ORDER_PAYMENT_COVERAGE",
    status=(
        "PASS"
        if completed_without_payment == 0
        else "FAIL"
    ),
    actual_value=completed_without_payment,
    expected_value=0,
    message=(
        "Every completed order has a paid payment"
        if completed_without_payment == 0
        else (
            f"{completed_without_payment} completed orders "
            "do not have a corresponding paid payment"
        )
    )
)


# =========================================================
# 5. Cancelled order ↔ refunded payment relationship
# =========================================================

cancelled_orders_df = (
    orders_df
    .filter(col("status") == "CANCELLED")
)

refunded_payments_df = (
    payments_df
    .filter(col("payment_status") == "REFUNDED")
)

cancelled_without_refund = (
    cancelled_orders_df.alias("o")
    .join(
        refunded_payments_df.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left_anti"
    )
    .count()
)

record_check(
    check_name="CANCELLED_ORDER_REFUND_COVERAGE",
    status=(
        "PASS"
        if cancelled_without_refund == 0
        else "FAIL"
    ),
    actual_value=cancelled_without_refund,
    expected_value=0,
    message=(
        "Every cancelled order has a refunded payment"
        if cancelled_without_refund == 0
        else (
            f"{cancelled_without_refund} cancelled orders "
            "do not have a corresponding refunded payment"
        )
    )
)


# =========================================================
# 6. Order amount vs payment amount reconciliation
# =========================================================

order_amounts_df = (
    orders_df
    .groupBy("order_id")
    .agg(
        spark_round(
            spark_sum(
                col("quantity") * col("unit_price")
            ),
            2
        ).alias("order_amount")
    )
)

payment_amounts_df = (
    payments_df
    .groupBy("order_id")
    .agg(
        spark_round(
            spark_sum("amount"),
            2
        ).alias("payment_amount")
    )
)

amount_mismatches = (
    order_amounts_df.alias("o")
    .join(
        payment_amounts_df.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "inner"
    )
    .filter(
        spark_round(
            col("o.order_amount") - col("p.payment_amount"),
            2
        ) != 0
    )
    .count()
)

record_check(
    check_name="ORDER_PAYMENT_AMOUNT_RECONCILIATION",
    status="PASS" if amount_mismatches == 0 else "FAIL",
    actual_value=amount_mismatches,
    expected_value=0,
    message=(
        "Order values match payment amounts"
        if amount_mismatches == 0
        else f"{amount_mismatches} orders have amount mismatches"
    )
)


# =========================================================
# 7. Completed sales total reconciliation
# =========================================================

completed_order_total = (
    completed_orders_df
    .select(
        spark_round(
            spark_sum(
                col("quantity") * col("unit_price")
            ),
            2
        ).alias("total")
    )
    .collect()[0]["total"]
)

paid_payment_total = (
    paid_payments_df
    .select(
        spark_round(
            spark_sum("amount"),
            2
        ).alias("total")
    )
    .collect()[0]["total"]
)

completed_order_total = (
    float(completed_order_total)
    if completed_order_total is not None
    else 0.0
)

paid_payment_total = (
    float(paid_payment_total)
    if paid_payment_total is not None
    else 0.0
)

sales_total_difference = round(
    completed_order_total - paid_payment_total,
    2
)

record_check(
    check_name="COMPLETED_SALES_TOTAL_RECONCILIATION",
    status=(
        "PASS"
        if sales_total_difference == 0
        else "FAIL"
    ),
    actual_value=completed_order_total,
    expected_value=paid_payment_total,
    message=(
        "Completed order sales equal paid payment total"
        if sales_total_difference == 0
        else (
            f"Sales/payment difference = "
            f"{sales_total_difference}"
        )
    )
)


# =========================================================
# 8. Payment duplication check
# =========================================================

duplicate_payment_orders = (
    paid_payments_df
    .groupBy("order_id")
    .agg(
        count("*").alias("payment_count")
    )
    .filter(col("payment_count") > 1)
    .count()
)

record_check(
    check_name="PAID_PAYMENT_DUPLICATION",
    status=(
        "PASS"
        if duplicate_payment_orders == 0
        else "FAIL"
    ),
    actual_value=duplicate_payment_orders,
    expected_value=0,
    message=(
        "No order has multiple paid payment records"
        if duplicate_payment_orders == 0
        else (
            f"{duplicate_payment_orders} orders have "
            "multiple paid payment records"
        )
    )
)


# =========================================================
# Summary
# =========================================================

total_checks = len(reconciliation_results)

passed_checks = sum(
    1
    for result in reconciliation_results
    if result["status"] == "PASS"
)

failed_checks = sum(
    1
    for result in reconciliation_results
    if result["status"] == "FAIL"
)

print("\n" + "=" * 80)
print("RECONCILIATION SUMMARY")
print("=" * 80)

print(f"Total checks : {total_checks}")
print(f"Passed checks: {passed_checks}")
print(f"Failed checks: {failed_checks}")

# =========================================================
# Fail pipeline when reconciliation fails
# =========================================================

if critical_failures:

    print("\nCRITICAL RECONCILIATION FAILURE")
    print("-" * 80)

    for failure in critical_failures:
        print(
            f"Check={failure['check']} | "
            f"Actual={failure['actual']} | "
            f"Expected={failure['expected']} | "
            f"{failure['message']}"
        )

    print("-" * 80)
    print("Gold publishing must not continue.")

    raise RuntimeError(
        "Data reconciliation failed: "
        f"{failed_checks} check(s) failed."
    )

# =========================================================
# Successful completion
# =========================================================

print("\nAll reconciliation checks passed.")
print("Silver data is reconciled and approved for Gold publishing.")

dbutils.notebook.exit(
    "RECONCILIATION_PASSED"
)
