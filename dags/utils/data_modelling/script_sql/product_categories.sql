-- Product Categories
CREATE TABLE data_modelling.product_categories (
    id INT PRIMARY KEY,
    name VARCHAR(50),
);
INSERT INTO data_modelling.product_categories (id, name)
SELECT id, name
FROM public.product_categories;