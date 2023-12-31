-- PRODUCTS
CREATE TABLE data_modelling.dim_products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    category_id INT,
    supplier_id INT,
    FOREIGN KEY (category_id) REFERENCES data_modelling.product_categories(id),
    FOREIGN KEY (supplier_id) REFERENCES data_modelling.suppliers(id)
);
INSERT INTO data_modelling.dim_products (id, name, price, category_id, supplier_id)
SELECT 
    id, 
    name, 
    price, 
    category_id, 
    supplier_id
FROM public.products;