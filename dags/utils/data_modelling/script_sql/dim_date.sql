-- Date
CREATE TABLE data_modelling.dim_date (
    id SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT
);

INSERT INTO data_modelling.dim_date (day, month, year)
SELECT
    EXTRACT(DAY FROM created_at) AS day,
    EXTRACT(MONTH FROM created_at) AS month,
    EXTRACT(YEAR FROM created_at) AS year
FROM public.orders;

