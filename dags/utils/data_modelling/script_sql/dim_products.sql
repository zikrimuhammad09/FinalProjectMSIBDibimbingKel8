-- PRODUCTS
CREATE TABLE data_modelling.dim_products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    category_id INT,
    supplier_id INT,
    FOREIGN KEY (category_id) REFERENCES PRODUCT_CATEGORIES(id),
    FOREIGN KEY (supplier_id) REFERENCES SUPPLIERS(id)
);
INSERT INTO data_modelling.dim_products (id, name, price, category_id, supplier_id)
SELECT id, name, price, category_id, supplier_id
FROM public.products;