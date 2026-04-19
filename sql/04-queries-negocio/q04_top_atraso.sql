--4. Top 10 créditos con mayor atraso y saldo pendiente.
CREATE OR REPLACE VIEW analytics_fintrust.q04_top_atraso AS

SELECT 
    loan_id,
    customer_name,
    customer_segment,
    SUM(overdue_principal) AS total_vencido,
    SUM(outstanding_principal + overdue_principal) AS saldo_total_pendiente,
    COUNT(CASE WHEN installment_status = 'LATE' THEN 1 END) AS cuotas_en_mora
FROM analytics_fintrust.dm_fintrust_performance 
GROUP BY 1, 2, 3
HAVING total_vencido > 0
ORDER BY total_vencido DESC
LIMIT 10;