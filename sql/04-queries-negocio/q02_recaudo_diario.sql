--2. Recaudo diario total y recaudo aplicado a cuotas vencidas.
CREATE OR REPLACE VIEW analytics_fintrust.q02_recaudo_diario AS

SELECT 
    collection_date,
    SUM(amount_collected) AS recaudo_total_diario,
    SUM(CASE WHEN installment_status = 'LATE' THEN amount_collected ELSE 0 END) AS recaudo_aplicado_mora,
    SAFE_DIVIDE(
        SUM(CASE WHEN installment_status = 'LATE' THEN amount_collected ELSE 0 END), 
        SUM(amount_collected)
    ) * 100 AS porcentaje_recaudo_mora
FROM analytics_fintrust.dm_fintrust_performance 
WHERE collection_date IS NOT NULL
GROUP BY 1
ORDER BY collection_date DESC;