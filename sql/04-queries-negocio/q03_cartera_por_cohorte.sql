--3. Cartera al día vs cartera en mora por cohorte de originación.
CREATE OR REPLACE VIEW analytics_fintrust.q03_cartera_por_cohorte AS
SELECT 
    cohort_month,
    SUM(outstanding_principal) AS cartera_al_dia,
    SUM(overdue_principal) AS cartera_en_mora,
    SUM(overdue_principal) / NULLIF(SUM(outstanding_principal + overdue_principal), 0) * 100 AS indice_mora_cohorte
FROM analytics_fintrust.dm_fintrust_performance 
GROUP BY 1
ORDER BY cohort_month ASC;