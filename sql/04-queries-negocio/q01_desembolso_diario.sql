--1. Desembolso total por día, ciudad y segmento.
CREATE OR REPLACE VIEW analytics_fintrust.q01_desembolso_diario AS
SELECT origination_date, customer_city, customer_segment, SUM(total_loan_disbursement) as total_desembolsado
FROM analytics_fintrust.dm_fintrust_performance 
GROUP BY 1, 2, 3
ORDER BY origination_date DESC, total_desembolsado DESC;