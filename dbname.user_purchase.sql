-- Table: dbname.products

CREATE SCHEMA IF NOT EXISTS dbname;

DROP TABLE IF EXISTS dbname.user_purchase;

CREATE TABLE IF NOT EXISTS dbname.user_purchase
(
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10,5),
    customer_id INT,
    country VARCHAR(30) 
);