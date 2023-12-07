-- Customer Conversion Rates
CREATE TABLE data_modelling.fact_customer_conversion_rates (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    number_of_purchases INT,
    number_of_sessions INT,
    coversion_rate DECIMAL(5,2),
    FOREIGN KEY (customer_id) REFERENCES data_modelling.dim_customers(id)
);

INSERT INTO data_modelling.fact_customer_conversion_rates (customer_id, number_of_purchases, number_of_sessions, coversion_rate)
SELECT
    l.customer_id,
    COUNT(DISTINCT o.id) AS number_of_purchases,
    COUNT(DISTINCT l.id) AS number_of_sessions,
    COALESCE((COUNT(DISTINCT o.id) * 100.0 / NULLIF(COUNT(DISTINCT l.id), 0)), 0) AS conversion_rate
FROM
    public.login_attempt_history l
LEFT JOIN
    public.orders o ON l.customer_id = o.customer_id
GROUP BY
    l.customer_id;
    