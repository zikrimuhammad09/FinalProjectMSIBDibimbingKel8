-- CONVERSION_RATES (PER TIME)
CREATE TABLE CONVERSION_RATES (
    id SERIAL PRIMARY KEY,
    time VARCHAR(7),
    -- Assuming "YYYY-MM" format for month and year
    purchases INT,
    sessions INT,
    conversion_rate DECIMAL(5, 2)
);

INSERT INTO
    CONVERSION_RATES (time, purchases, sessions, conversion_rate)
SELECT
    TO_CHAR(DATE_TRUNC('month', o.created_at), 'YYYY-MM') AS time,
    COUNT(DISTINCT oi.order_id) AS purchases,
    COUNT(DISTINCT l.id) AS sessions,
    (
        COUNT(DISTINCT oi.order_id) / NULLIF(COUNT(DISTINCT l.id), 0)
    ) * 100 AS conversion_rate
FROM
    LOGIN_ATTEMPT_HISTORY l
    LEFT JOIN ORDERS o ON l.customer_id = o.customer_id
    LEFT JOIN ORDER_ITEMS oi ON o.id = oi.order_id
GROUP BY
    TO_CHAR(DATE_TRUNC('month', o.created_at), 'YYYY-MM');

-- CONVERSION_RATES (PER CUSTOMER)
CREATE TABLE CONVERSION_RATES_CUSTOMER (
    customer_id INT PRIMARY KEY,
    number_of_purchases INT,
    number_of_sessions INT,
    conversion_rate DECIMAL(5, 2)
);

INSERT INTO
    CONVERSION_RATES_CUSTOMER (
        customer_id,
        number_of_purchases,
        number_of_sessions,
        conversion_rate
    )
SELECT
    l.customer_id,
    COUNT(DISTINCT o.id) AS number_of_purchases,
    COUNT(DISTINCT l.id) AS number_of_sessions,
    COALESCE(
        (
            COUNT(DISTINCT o.id) * 100.0 / NULLIF(COUNT(DISTINCT l.id), 0)
        ),
        0
    ) AS conversion_rate
FROM
    LOGIN_ATTEMPT_HISTORY l
    LEFT JOIN ORDERS o ON l.customer_id = o.customer_id
GROUP BY
    l.customer_id;

-- SALES
CREATE TABLE SALES (
    id SERIAL PRIMARY KEY,
    order_id INT,
    product_id INT,
    customer_id INT,
    price DECIMAL(19, 4),
    amount INT,
    discount_percent DECIMAL(10, 2),
    total_price DECIMAL(19, 4),
    order_status VARCHAR(255),
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES CUSTOMERS(id),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(id)
);

INSERT INTO
    SALES (
        order_id,
        product_id,
        customer_id,
        price,
        amount,
        discount_percent,
        total_price,
        order_status,
        order_date
    )
SELECT
    oi.order_id,
    oi.product_id,
    o.customer_id,
    p.price,
    oi.amount,
    COALESCE(c.discount_percent, 0) AS discount_percent,
    (
        p.price * oi.amount * (1 - COALESCE(c.discount_percent, 0) / 100.0)
    ) AS total_price,
    o.status AS order_status,
    o.created_at :: DATE AS order_date
FROM
    ORDER_ITEMS oi
    JOIN PRODUCTS p ON oi.product_id = p.id
    JOIN ORDERS o ON oi.order_id = o.id
    LEFT JOIN COUPONS c ON oi.coupon_id = c.id;

-- POPULAR_PRODUCTS
CREATE TABLE popular_products (
    id SERIAL PRIMARY KEY,
    product_id INT,
    product_name VARCHAR(100),
    category_id INT,
    total_quantity_sold INT,
    total_revenue_generated DECIMAL(19, 4),
    FOREIGN KEY (product_id) REFERENCES PRODUCTS(id),
    FOREIGN KEY (category_id) REFERENCES PRODUCT_CATEGORIES(id)
);

INSERT INTO
    popular_products (
        product_id,
        product_name,
        category_id,
        total_quantity_sold,
        total_revenue_generated
    )
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.category_id,
    count(oi.amount) AS total_quantity_sold,
    SUM(
        p.price * oi.amount - (
            p.price * oi.amount * COALESCE(c.discount_percent, 0) / 100.0
        )
    ) AS total_revenue_generated
FROM
    PRODUCTS p
    JOIN ORDER_ITEMS oi ON p.id = oi.product_id
    LEFT JOIN COUPONS c ON oi.coupon_id = c.id
GROUP BY
    p.id,
    p.name,
    p.category_id
ORDER BY
    total_quantity_sold DESC;

--kontribusi supplier terhadap revenue penjualan produk
create table contribution_supplier(
    supplier_name varchar,
    country varchar,
    product_category varchar,
    total_product_sales int
);

insert into
    contribution_supplier(
        supplier_name,
        country,
        product_category,
        total_product_sales
    )
select
    s."name",
    s.country,
    pc."name" as product_category,
    count(o.id) as jumlah_pembelian
from
    suppliers s
    inner join products p on p.supplier_id = s.id
    inner join product_categories pc on pc.id = p.category_id
    inner join order_items oi on oi.product_id = p.id
    inner join orders o on o.id = oi.order_id
    inner join customers c on c.id = o.customer_id
group by
    s.id,
    pc."name"
order by
    jumlah_pembelian desc;

--average order value
create table AVERAGE_ORDER_VALUE(total_revenue int, "date" date);

insert into
    average_order_value(total_revenue, "date")
select
    sum(p.price * oi.amount) / count(o.id) as total_revenue,
    o.created_at
from
    products p
    inner join order_items oi on oi.product_id = p.id
    inner join orders o on o.id = oi.order_id
group by
    o.created_at 