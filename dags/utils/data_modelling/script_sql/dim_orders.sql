-- Dim Orders
-- Hapus jika ada type order_status
DROP TYPE IF EXISTS order_status;

CREATE TYPE order_status AS ENUM ('PROCESSED', 'SENT', 'RECEIVED', 'FINISHED', 'RETURNED', 'ABORTED');

CREATE TABLE data_modelling.dim_orders (
    id INT PRIMARY KEY,
    status order_status
);

INSERT INTO data_modelling.dim_orders (id, status)
SELECT 
    id, 
    status::order_status
FROM public.orders;


