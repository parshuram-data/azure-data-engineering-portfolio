from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

spark = SparkSession.builder.appName("RetailDataQualityChecks").getOrCreate()

# ---------------------------------------------------------
# Parameters
# ---------------------------------------------------------

dbutils.widgets.text("silver_path", "silver")
silver_path = dbutils.widgets.get("silver_path").strip()

if not silver_path:
    raise ValueError("silver_path parameter cannot be empty")

print(f"Starting data quality checks")
print(f"Silver path: {silver_path}")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

TABLE_CONFIG = {
    "customers": {
        "primary_key": "customer_id",
        "required_columns": [
            "customer_id",
            "customer_name",
            "city",
            "state",
            "signup_date"
        ]
    },
    "products": {
        "primary_key": "product_id",
        "required_columns": [
            "product_id",
            "product_name",
            "category",
            "price"
        ]
    },
    "orders": {
        "primary_key": "order_id",
        "required_columns": [
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
            "status"
        ]
    },
    "payments": {
        "primary_key": "payment_id",
        "required_columns": [
            "payment_id",
            "order_id",
            "payment_date",
            "payment_method",
            "amount",
            "payment_status"
        ]
    }
}

VALID_ORDER_STATUSES = {
    "COMPLETED",
    "CANCELLED"
}

VALID_PAYMENT_STATUSES = {
    "PAID",
    "REFUNDED"
}

# ---------------------------------------------------------
# Results
# ---------------------------------------------------------

dq_results = []
critical_failures = []


def record_check(
    table_name,
    check_name,
    status,
    actual_value,
    expected_value,
    message
):
    result = {
        "table": table_name,
        "check": check_name,
        "status": status,
        "actual": actual_value,
        "expected": expected_value,
        "message": message
    }

    dq_results.append(result)

    if status == "PASS":
        print(
            f"[PASS] {table_name} | {check_name} | {message}"
        )
    else:
        print(
            f"[FAIL] {table_name} | {check_name} | {message}"
        )

        critical_failures.append(result)


# ---------------------------------------------------------
# Check each Silver table
# ---------------------------------------------------------

for table_name, config in TABLE_CONFIG.items():

    table_path = f"{silver_path}/{table_name}"

    print("\n" + "=" * 80)
    print(f"Checking Silver table: {table_name}")
    print(f"Path: {table_path}")
    print("=" * 80)

    # -----------------------------------------------------
    # Table existence
    # -----------------------------------------------------

    try:
        df = spark.read.format("delta").load(table_path)

    except Exception as exc:
        record_check(
            table_name=table_name,
            check_name="TABLE_EXISTS",
            status="FAIL",
            actual_value="NOT_FOUND",
            expected_value="DELTA_TABLE",
            message=f"Unable to read Silver table: {str(exc)}"
        )

        # Cannot perform remaining checks for this table.
        continue

    record_check(
        table_name=table_name,
        check_name="TABLE_EXISTS",
        status="PASS",
        actual_value="FOUND",
        expected_value="DELTA_TABLE",
        message="Silver Delta table is readable"
    )

    # -----------------------------------------------------
    # Required columns
    # -----------------------------------------------------

    actual_columns = set(df.columns)

    missing_columns = [
        column_name
        for column_name in config["required_columns"]
        if column_name not in actual_columns
    ]

    if missing_columns:

        record_check(
            table_name=table_name,
            check_name="REQUIRED_COLUMNS",
            status="FAIL",
            actual_value=str(sorted(actual_columns)),
            expected_value=str(config["required_columns"]),
            message=f"Missing required columns: {missing_columns}"
        )

        # Remaining checks depend on the expected schema.
        continue

    record_check(
        table_name=table_name,
        check_name="REQUIRED_COLUMNS",
        status="PASS",
        actual_value=str(sorted(actual_columns)),
        expected_value=str(config["required_columns"]),
        message="All required columns are present"
    )

    # -----------------------------------------------------
    # Record count
    # -----------------------------------------------------

    record_count = df.count()

    record_check(
        table_name=table_name,
        check_name="RECORD_COUNT",
        status="PASS" if record_count > 0 else "FAIL",
        actual_value=record_count,
        expected_value="> 0",
        message=(
            f"Table contains {record_count} records"
            if record_count > 0
            else "Table contains no records"
        )
    )

    # -----------------------------------------------------
    # Primary key null check
    # -----------------------------------------------------

    primary_key = config["primary_key"]

    null_pk_count = (
        df.filter(
            col(primary_key).isNull()
            | (trim(col(primary_key).cast("string")) == "")
        )
        .count()
    )

    record_check(
        table_name=table_name,
        check_name="PRIMARY_KEY_NOT_NULL",
        status="PASS" if null_pk_count == 0 else "FAIL",
        actual_value=null_pk_count,
        expected_value=0,
        message=(
            "No null or blank primary keys found"
            if null_pk_count == 0
            else f"Found {null_pk_count} null or blank primary keys"
        )
    )

    # -----------------------------------------------------
    # Duplicate primary key check
    # -----------------------------------------------------

    duplicate_pk_count = (
        df.groupBy(primary_key)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    record_check(
        table_name=table_name,
        check_name="PRIMARY_KEY_UNIQUENESS",
        status="PASS" if duplicate_pk_count == 0 else "FAIL",
        actual_value=duplicate_pk_count,
        expected_value=0,
        message=(
            "Primary key is unique"
            if duplicate_pk_count == 0
            else f"Found {duplicate_pk_count} duplicated primary-key values"
        )
    )

    # -----------------------------------------------------
    # Table-specific checks
    # -----------------------------------------------------

    if table_name == "customers":

        invalid_dates = (
            df.filter(col("signup_date").isNull())
            .count()
        )

        record_check(
            table_name,
            "SIGNUP_DATE_VALID",
            "PASS" if invalid_dates == 0 else "FAIL",
            invalid_dates,
            0,
            (
                "All signup dates are populated"
                if invalid_dates == 0
                else f"Found {invalid_dates} null signup dates"
            )
        )

    elif table_name == "products":

        invalid_price_count = (
            df.filter(
                col("price").isNull()
                | (col("price") < 0)
            )
            .count()
        )

        record_check(
            table_name,
            "PRICE_VALID",
            "PASS" if invalid_price_count == 0 else "FAIL",
            invalid_price_count,
            0,
            (
                "All product prices are valid"
                if invalid_price_count == 0
                else f"Found {invalid_price_count} invalid product prices"
            )
        )

    elif table_name == "orders":

        invalid_quantity_count = (
            df.filter(
                col("quantity").isNull()
                | (col("quantity") <= 0)
            )
            .count()
        )

        record_check(
            table_name,
            "QUANTITY_VALID",
            "PASS" if invalid_quantity_count == 0 else "FAIL",
            invalid_quantity_count,
            0,
            (
                "All order quantities are greater than zero"
                if invalid_quantity_count == 0
                else f"Found {invalid_quantity_count} invalid quantities"
            )
        )

        invalid_unit_price_count = (
            df.filter(
                col("unit_price").isNull()
                | (col("unit_price") < 0)
            )
            .count()
        )

        record_check(
            table_name,
            "UNIT_PRICE_VALID",
            "PASS" if invalid_unit_price_count == 0 else "FAIL",
            invalid_unit_price_count,
            0,
            (
                "All unit prices are valid"
                if invalid_unit_price_count == 0
                else f"Found {invalid_unit_price_count} invalid unit prices"
            )
        )

        invalid_status_count = (
            df.filter(
                col("status").isNull()
                | (~upper(trim(col("status"))).isin(
                    list(VALID_ORDER_STATUSES)
                ))
            )
            .count()
        )

        record_check(
            table_name,
            "ORDER_STATUS_VALID",
            "PASS" if invalid_status_count == 0 else "FAIL",
            invalid_status_count,
            0,
            (
                "All order statuses are valid"
                if invalid_status_count == 0
                else f"Found {invalid_status_count} invalid order statuses"
            )
        )

    elif table_name == "payments":

        invalid_amount_count = (
            df.filter(
                col("amount").isNull()
                | (col("amount") < 0)
            )
            .count()
        )

        record_check(
            table_name,
            "PAYMENT_AMOUNT_VALID",
            "PASS" if invalid_amount_count == 0 else "FAIL",
            invalid_amount_count,
            0,
            (
                "All payment amounts are valid"
                if invalid_amount_count == 0
                else f"Found {invalid_amount_count} invalid payment amounts"
            )
        )

        invalid_status_count = (
            df.filter(
                col("payment_status").isNull()
                | (~upper(trim(col("payment_status"))).isin(
                    list(VALID_PAYMENT_STATUSES)
                ))
            )
            .count()
        )

        record_check(
            table_name,
            "PAYMENT_STATUS_VALID",
            "PASS" if invalid_status_count == 0 else "FAIL",
            invalid_status_count,
            0,
            (
                "All payment statuses are valid"
                if invalid_status_count == 0
                else f"Found {invalid_status_count} invalid payment statuses"
            )
        )

# ---------------------------------------------------------
# Summary
# ---------------------------------------------------------

total_checks = len(dq_results)
passed_checks = sum(
    1 for result in dq_results
    if result["status"] == "PASS"
)
failed_checks = sum(
    1 for result in dq_results
    if result["status"] == "FAIL"
)

print("\n" + "=" * 80)
print("DATA QUALITY SUMMARY")
print("=" * 80)

print(f"Total checks : {total_checks}")
print(f"Passed checks: {passed_checks}")
print(f"Failed checks: {failed_checks}")

# ---------------------------------------------------------
# Fail the notebook if any critical check failed
# ---------------------------------------------------------

if critical_failures:

    print("\nCRITICAL DATA QUALITY FAILURE")
    print("-" * 80)

    for failure in critical_failures:
        print(
            f"Table={failure['table']} | "
            f"Check={failure['check']} | "
            f"Actual={failure['actual']} | "
            f"Expected={failure['expected']} | "
            f"Message={failure['message']}"
        )

    print("-" * 80)
    print("Gold publishing must not continue.")

    raise RuntimeError(
        f"Data quality validation failed: "
        f"{failed_checks} check(s) failed."
    )

# ---------------------------------------------------------
# Successful completion
# ---------------------------------------------------------

print("\nAll data quality checks passed.")
print("Silver data is approved for Gold publishing.")

dbutils.notebook.exit(
    "DATA_QUALITY_PASSED"
)
