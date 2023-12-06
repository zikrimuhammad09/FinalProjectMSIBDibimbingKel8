-- COUPONS
CREATE TABLE data_modelling.dim_coupons (
    id INT PRIMARY KEY,
    discount_percent DECIMAL(5, 2)
);
INSERT INTO data_modelling.dim_coupons (id, discount_percent)
SELECT id, discount_percent
FROM public.coupons;