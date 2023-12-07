-- Fact Sales
CREATE TABLE data_modelling.fact_sales (
    id SERIAL PRIMARY KEY,
    order_id INT,
    product_id INT,
    customer_id INT,
    coupon_id INT,
    amount INT,
    total_price INT,
    purchase_dates TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES data_modelling.dim_orders(id),
    FOREIGN KEY (product_id) REFERENCES data_modelling.dim_products(id),
    FOREIGN KEY (coupon_id) REFERENCES data_modelling.dim_coupons(id),
    FOREIGN KEY (customer_id) REFERENCES data_modelling.dim_customers(id),
    FOREIGN KEY (purchase_dates) REFERENCES data_modelling.dim_date(purchase_dates)
);

INSERT INTO data_modelling.fact_sales(order_id,product_id,customer_id,coupon_id,amount,total_price,purchase_dates)
SELECT 
    oi.order_id,
    oi.product_id,
    o.customer_id,
    oi.coupon_id,
    oi.amount,
    (p.price * oi.amount * (1 - COALESCE(c.discount_percent, 0) / 100.0)) AS total_price,
    o.created_at
FROM public.order_items oi
JOIN public.orders o ON oi.order_id = o.id
JOIN public.products p on oi.product_id = p.id
JOIN public.coupons c ON oi.coupon_id = c.id;

