CREATE SCHEMA deb_paulirat;
CREATE TABLE deb_paulirat.user_purchase(
invoice_number varchar(10),
stock_code varchar(20),
detail varchar(1000),
quantity int,
invoice_date timestamp,
unit_price numeric(8,3),
customer_id int,
country varchar(20)
);