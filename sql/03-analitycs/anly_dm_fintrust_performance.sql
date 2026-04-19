CREATE OR REPLACE TABLE analytics_fintrust.dm_fintrust_performance AS
WITH payment_agg AS (
    -- Agrupamos pagos por cuota para evitar duplicidad de filas si hay abonos parciales
    SELECT 
        installment_id,
        MAX(payment_date) AS last_payment_date,
        SUM(payment_amount) AS total_paid_amount,
        STRING_AGG(DISTINCT payment_channel, ', ') AS payment_channels
    FROM stg_fintrust.stg_payments
    WHERE payment_status = 'CONFIRMED'
    GROUP BY 1
)
SELECT
    inst.installment_id,
    inst.loan_id,
    cust.customer_id,

    cust.full_name AS customer_name,
    cust.city AS customer_city,
    cust.segment AS customer_segment,

    ln.origination_date,
    ln.origination_cohort AS cohort_month,
    ln.product_type,
    ln.loan_status,
    -- Marcamos el monto solo en la primera cuota para evitar duplicar el desembolso al sumar
    CASE WHEN inst.installment_number = 1 THEN ln.principal_amount ELSE 0 END AS total_loan_disbursement,

    inst.installment_number,
    inst.due_date,
    inst.principal_due,
    inst.interest_due,
    inst.total_installment_amount,
    inst.installment_status,
    CASE WHEN inst.installment_status = 'LATE' THEN inst.principal_due 
    ELSE 0 
    END AS overdue_principal,

    CASE WHEN inst.installment_status IN ('DUE', 'PARTIAL') 
    THEN inst.principal_due 
    ELSE 0 END AS outstanding_principal,

    COALESCE(pay.total_paid_amount, 0) AS amount_collected,
    pay.last_payment_date AS collection_date,
    
    -- Bandera de Deterioro Temprano. Se realiza el supuesto de que este empieza en los 12 primeros meses
    CASE 
        WHEN inst.installment_status = 'LATE' 
        AND DATE_DIFF(inst.due_date, ln.origination_date, MONTH) <= 12 
        THEN 1 ELSE 0 
    END AS is_early_deterioration

FROM stg_fintrust.stg_installments inst
JOIN stg_fintrust.stg_loans ln ON inst.loan_id = ln.loan_id
JOIN stg_fintrust.stg_customers cust ON ln.customer_id = cust.customer_id
LEFT JOIN payment_agg pay ON inst.installment_id = pay.installment_id;