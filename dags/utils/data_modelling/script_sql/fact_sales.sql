-- Fact Sales
CREATE TABLE data_modelling.fact_sales (
    id SERIAL PRIMARY KEY,
    order_id INT,
    product_id INT,
    customer_id INT,
    coupon_id INT,
    amount INT,
    total_price INT,
    date_id TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES dim_orders(id),
    FOREIGN KEY (product_id) REFERENCES dim_products(id),
    FOREIGN KEY (coupon_id) REFERENCES COUPONS(id),
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(id),
    FOREIGN KEY (date_id) REFERENCES DIM_DATE(date_id)
);

INSERT INTO data_modelling.fact_sales(order_id,product_id,customer_id,coupon_id,amount,total_price,date_id)
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

