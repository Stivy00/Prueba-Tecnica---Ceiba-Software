--Query que genera cargue de nuevos datos hacia las tablas raw.

INSERT INTO raw_fintrust.customers VALUES
('C036','Ana Perez','Bogota','Mass Market',4200000,'2024-11-10'),
('C037','Luis Alcino','Medellin','Premium',8600000,'2024-12-02');

INSERT INTO raw_fintrust.loans VALUES
('L046','C036','2025-02-05',24000000,0.24,12,'ACTIVE','Payroll'),
('L047','C037','2025-06-12',35000000,0.21,18,'ACTIVE','Digital');

INSERT INTO raw_fintrust.installments VALUES
('I136','L046',1,'2025-02-05',1000000,240000,'PAID'),
('I137','L047',2,'2025-03-05',1000000,220000,'LATE');

INSERT INTO raw_fintrust.payments VALUES
('P108','L046','I136','2025-02-05',1340000,'PSE','CONFIRMED',CURRENT_TIMESTAMP()),
('P109','L047','I137','2025-02-13',1926389,'ACH','CONFIRMED',CURRENT_TIMESTAMP());

UPDATE raw_fintrust.payments
SET payment_status = 'CONFIRMED', loaded_at = CURRENT_TIMESTAMP()
WHERE payment_id = 'P105';


UPDATE raw_fintrust.payments
SET payment_channel = 'CASH', loaded_at = CURRENT_TIMESTAMP()
WHERE payment_id = 'P102';
