-- Date
CREATE TABLE data_modelling.dim_date (
    purchase_dates TIMESTAMP PRIMARY KEY,
    day INT,
    month INT,
    year INT
);

INSERT INTO data_modelling.dim_date (purchase_dates, day, month, year)
SELECT
    DISTINCT created_at AS purchase_dates,
    EXTRACT(DAY FROM created_at) AS day,
    EXTRACT(MONTH FROM created_at) AS month,
    EXTRACT(YEAR FROM created_at) AS year
FROM public.orders
ORDER BY created_at;

