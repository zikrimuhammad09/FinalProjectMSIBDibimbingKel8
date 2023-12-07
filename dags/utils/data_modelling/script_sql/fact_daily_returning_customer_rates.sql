CREATE TABLE data_modelling.fact_daily_returning_customer_rates (
    id SERIAL PRIMARY KEY,
    date timestamp,
    returning_customers INT,
    total_customers INT,
    returning_customer_rate DECIMAL(5, 2),
    FOREIGN KEY (date) REFERENCES data_modelling.dim_date(purchase_dates)
);

INSERT INTO data_modelling.fact_daily_returning_customer_rates (date, returning_customers, total_customers, returning_customer_rate)
SELECT
    o.created_at AS date,
    COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) AS returning_customers,
    COUNT(DISTINCT customer_id) AS total_customers,
    (COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) / NULLIF(COUNT(DISTINCT customer_id), 0)) * 100 AS returning_customer_rate
FROM (
    SELECT
        customer_id,
        TO_DATE(CONCAT(d.year, '-', d.month, '-', d.day), 'YYYY-MM-DD') AS created_at,
        COUNT(DISTINCT o.id) AS order_count
    FROM public.orders o
    JOIN data_modelling.dim_date d ON DATE(o.created_at) = TO_DATE(CONCAT(d.year, '-', d.month, '-', d.day), 'YYYY-MM-DD')
    GROUP BY customer_id, TO_DATE(CONCAT(d.year, '-', d.month, '-', d.day), 'YYYY-MM-DD')
) o
GROUP BY o.created_at
ORDER BY o.created_at;