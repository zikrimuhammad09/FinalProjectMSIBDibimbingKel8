-- LOGIN_ATTEMPT_HISTORY
CREATE TABLE data_modelling.fact_login_attempt_history (
    id INT PRIMARY KEY,
    customer_id INT,
    login_successfull BOOLEAN,
    attempted_at TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES data_modelling.dim_customers(id)
);
INSERT INTO data_modelling.fact_login_attempt_history (id, customer_id, login_successfull, attempted_at)
SELECT 
    id, 
    customer_id, 
    login_successfull, 
    attempted_at
FROM public.login_attempt_history;