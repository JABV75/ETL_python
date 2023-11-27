DROP TABLE D_clientes
CREATE TABLE D_clientes (
    PARTY_NAME VARCHAR(100) PRIMARY KEY,
    LOCATION_SHIP_TO VARCHAR(50)
);
DROP TABLE D_product
CREATE TABLE D_product (
    SEGMENT1 INT PRIMARY KEY,
    DESCRIPTION_PRODUCT VARCHAR(MAX),
    UNIT_SELLING_PRICE FLOAT
);
DROP TABLE D_fecha
CREATE TABLE D_fecha (
    TRX_DATE DATE PRIMARY KEY,
    Month INT
);
DROP TABLE D_transaccion
CREATE TABLE D_transaccion (
    CUSTOMER_TRX_ID INT PRIMARY KEY,
	TRX_NUMBER INT,
    INVOICE_CURRENCY_CODE INT
);
DROP TABLE D_canal
CREATE TABLE D_canal (
    SALES_CHANNEL_CODE VARCHAR(50) PRIMARY KEY,
    SALES_ORDER_LINE INT,
    ATTRIBUTE10 VARCHAR(50),
    SALES_ORDER INT
);
DROP TABLE Fact_ventas
-- Crear la tabla de hechos con referencias a las claves primarias de las dimensiones y la nueva columna EXTENDED_AMOUNT
CREATE TABLE Fact_ventas (
    ID INT PRIMARY KEY,
    TRANSACTION_DATE DATE,---ddate
    PRODUCT_CODE INT,---dproducto
    SALES_CHANNEL_CODE VARCHAR(50),---dcanal,
    PARTY_NAME VARCHAR(100),--dclientes
    CUSTOMER_TRX_ID INT,-- d transaccion
	QUANTITY_INVOICED FLOAT, -- Nueva columna
    EXTENDED_AMOUNT FLOAT,  -- Nueva columna
    FOREIGN KEY (TRANSACTION_DATE) REFERENCES D_fecha(TRX_DATE),
    FOREIGN KEY (PRODUCT_CODE) REFERENCES D_product(SEGMENT1),
    FOREIGN KEY (SALES_CHANNEL_CODE) REFERENCES D_canal(SALES_CHANNEL_CODE),
    FOREIGN KEY (PARTY_NAME) REFERENCES D_clientes(PARTY_NAME),
    FOREIGN KEY (CUSTOMER_TRX_ID) REFERENCES D_transaccion(CUSTOMER_TRX_ID),
);

INSERT INTO D_clientes (PARTY_NAME, LOCATION_SHIP_TO) VALUES
('Cliente1', 'Ubicacion1'),
('Cliente2', 'Ubicacion2'),
('Cliente3', 'Ubicacion3');