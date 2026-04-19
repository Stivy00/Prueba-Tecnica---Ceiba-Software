--Se usa para generar la tabla staging customers que limpie de espacios las columnas customer_id y full_name, ademas de
--estandarizar los nombres de las ciudades y segmentos poniendolos en mayuscula. 

SELECT 
    TRIM(customer_id) AS customer_id,
    TRIM(full_name) AS full_name,
    UPPER(TRIM(city)) AS city,
    UPPER(TRIM(segment)) AS segment,
    monthly_income AS monthly_income,
    created_at
FROM raw_fintrust.customers