--Se usa para generar una tabla con los estados de los pagos estandarizados, ademas de identificar canales en Null como desconocidos
CREATE OR REPLACE TABLE stg_fintrust.stg_payments AS
SELECT
    payment_id AS payment_id,
    loan_id AS loan_id,
    installment_id AS installment_id,
    payment_date AS payment_date,
    payment_amount AS payment_amount,
    CASE 
        WHEN UPPER(payment_channel) IS NULL THEN 'DESCONOCIDO'
    ELSE UPPER(payment_channel) END AS payment_channel,
    TRIM(payment_status) AS payment_status,
    loaded_at
FROM raw_fintrust.payments;