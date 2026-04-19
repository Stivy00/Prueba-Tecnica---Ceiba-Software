CREATE SCHEMA IF NOT EXISTS stg_fintrust;

CREATE TABLE IF NOT EXISTS stg_fintrust.stg_customers (
    customer_id STRING,
    full_name STRING,
    city STRING,
    segment STRING,
    monthly_income NUMERIC,
    created_at DATE
);
CREATE TABLE IF NOT EXISTS stg_fintrust.stg_loans (
    loan_id STRING,
    customer_id STRING,
    origination_date DATE,
    principal_amount NUMERIC,
    annual_rate FLOAT64,
    term_months INT64,
    loan_status STRING,
    product_type STRING,
    origination_cohort STRING
);
CREATE TABLE IF NOT EXISTS stg_fintrust.stg_installments (
    installment_id STRING,
    loan_id STRING,
    installment_number INT64,
    due_date DATE,
    principal_due NUMERIC,
    interest_due NUMERIC,
    total_installment_amount NUMERIC,
    installment_status STRING,
    due_cohort STRING
);
CREATE TABLE IF NOT EXISTS stg_fintrust.stg_payments (
    payment_id STRING,
    loan_id STRING,
    installment_id STRING,
    payment_date DATE,
    payment_amount NUMERIC,
    payment_channel STRING,
    payment_status STRING,
    loaded_at TIMESTAMP
);  