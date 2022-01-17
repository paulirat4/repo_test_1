-- Table: dbname.products

CREATE SCHEMA IF NOT EXISTS dbname;

DROP TABLE IF EXISTS dbname.products;

CREATE TABLE IF NOT EXISTS dbname.products
(
    id SERIAL PRIMARY KEY,
    producto VARCHAR(255),
    presentacion VARCHAR(255),
    marca VARCHAR(255),
    categoria VARCHAR(255),
    catalogo VARCHAR(255),
    precio FLOAT,
    fecharegistro DATE,
    cadenacomercial VARCHAR(255),
    giro VARCHAR(255),
    nombrecomercial VARCHAR(255),
    direccion VARCHAR(255),
    estado VARCHAR(255),
    municipio VARCHAR(255),
    latitud FLOAT,
    longitud FLOAT    
);