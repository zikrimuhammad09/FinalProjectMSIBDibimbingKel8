-- SUPPLIERS
CREATE TABLE data_modelling.suppliers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50)
);
INSERT INTO data_modelling.suppliers (id, name, country)
SELECT id, name, country
FROM public.suppliers;