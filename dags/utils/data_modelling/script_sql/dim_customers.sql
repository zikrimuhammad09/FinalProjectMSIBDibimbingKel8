-- CUSTOMERS
CREATE TABLE data_modelling.dim_customers (
    id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(255),
    gender CHAR(1),
    zip_code VARCHAR(10)
);
INSERT INTO data_modelling.dim_customers (id, first_name, last_name, address, gender, zip_code)
SELECT id, first_name, last_name, address, gender, zip_code
FROM public.customers;