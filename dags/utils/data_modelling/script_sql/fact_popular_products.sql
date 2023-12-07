-- Popular Products
CREATE TABLE data_modelling.fact_popular_products (
    id SERIAL PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category_id INT,
    total_quantity_sold INT,
    total_revenue_generated DECIMAL(19, 4),
    FOREIGN KEY (product_id) REFERENCES data_modelling.dim_products(id),
    FOREIGN KEY (category_id) REFERENCES data_modelling.product_categories(id)
);

INSERT INTO data_modelling.fact_popular_products (product_id, product_name, category_id, total_quantity_sold, total_revenue_generated)
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.category_id,
    count(oi.amount) AS total_quantity_sold,
    SUM(p.price * oi.amount - (p.price * oi.amount * COALESCE(c.discount_percent, 0) / 100.0)) AS total_revenue_generated
FROM
    public.PRODUCTS p
JOIN
    public.ORDER_ITEMS oi ON p.id = oi.product_id
LEFT JOIN
    public.COUPONS c ON oi.coupon_id = c.id
GROUP BY
    p.id, p.name, p.category_id
ORDER BY
	total_quantity_sold DESC;