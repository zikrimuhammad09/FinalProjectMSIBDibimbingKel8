-- Dim Orders
CREATE TYPE order_status AS ENUM ('PROCESSED', 'SENT', 'RECEIVED', 'FINISHED', 'RETURN', 'ABORTED');

CREATE TABLE data_modelling.dim_orders (
    id INT PRIMARY KEY,
    status order_status
);

INSERT INTO data_modelling.dim_orders (id, status)
SELECT id, status
FROM public.orders;


