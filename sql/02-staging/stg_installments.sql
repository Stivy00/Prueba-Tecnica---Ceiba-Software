SELECT
    installment_id AS installment_id,
    loan_id AS loan_id,
    CAST(installment_number AS INT64) AS installment_number,
    due_date AS due_date,
    principal_due AS principal_due,
    interest_due,
    (principal_due + interest_due) AS total_installment_amount,
    UPPER(installment_status) AS installment_status,
    FORMAT_DATE('%Y-%m', CAST(due_date AS DATE)) AS due_cohort

FROM raw_fintrust.installments