--Se usa para generar la tabla staging de loans, con la cual se ajusta el tipo de datos a la columna annual_rate y term_months
-- Ademas de estandarizar la columna de product_type. Se genera un nuevo campo que indica el cohorte de origen, extrayendo unicamente el mes
SELECT
    loan_id AS loan_id,
    customer_id AS customer_id,
    origination_date AS origination_date,
    principal_amount AS principal_amount,
    CAST(annual_rate AS FLOAT64) AS annual_rate,
    CAST(term_months AS INT64) AS term_months,
    loan_status AS loan_status,
    UPPER(product_type) AS product_type,
    FORMAT_DATE('%Y-%m', CAST(origination_date AS DATE)) AS origination_cohort
FROM raw_fintrust.loans