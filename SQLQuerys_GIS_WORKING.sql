USE GIS
---DDL
--DROP TABLE D_clientes
CREATE TABLE D_clientes (
	CUST_ACCOUNT_ID FLOAT PRIMARY KEY,
    PARTY_NAME VARCHAR(MAX)
);
--DROP TABLE D_product
CREATE TABLE D_product (
    SEGMENT1 VARCHAR(150) PRIMARY KEY,
    DESCRIPTION_PRODUCT VARCHAR(MAX)
);
--DROP TABLE D_fecha
CREATE TABLE D_fecha (
    TRX_DATE DATE PRIMARY KEY,
    Month INT
);
--DROP TABLE D_canal
CREATE TABLE D_canal (
    SALES_CHANNEL_CODE VARCHAR(150) PRIMARY KEY
);
--DROP TABLE Fact_ventas
CREATE TABLE Fact_ventas (
    ID INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    CUST_ACCOUNT_ID FLOAT, -- dclientes
    TRX_DATE DATE,         -- ddate
    SEGMENT1 VARCHAR(150), -- dproducto SEGMENT1
    SALES_CHANNEL_CODE VARCHAR(150), -- dcanal
    QUANTITY_INVOICED FLOAT,       -- Nueva columna
    EXTENDED_AMOUNT FLOAT,         -- Nueva columna
    FOREIGN KEY (CUST_ACCOUNT_ID) REFERENCES D_clientes(CUST_ACCOUNT_ID),
    FOREIGN KEY (SEGMENT1) REFERENCES D_product(SEGMENT1),
    FOREIGN KEY (TRX_DATE) REFERENCES D_fecha(TRX_DATE),
    FOREIGN KEY (SALES_CHANNEL_CODE) REFERENCES D_canal(SALES_CHANNEL_CODE)
);

---DML
SELECT DISTINCT SEGMENT1 FROM dbo.[2023_02];
SELECT * FROM dbo.[2023_02] WHERE SEGMENT1 = 'Cargo Embarque'

-- Inserción en D_clientes
INSERT INTO D_clientes (CUST_ACCOUNT_ID, PARTY_NAME)
SELECT DISTINCT CUST_ACCOUNT_ID, PARTY_NAME
FROM
   (
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2019_01] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2019_02] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2020_01] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2020_02] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2021_01] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2021_02] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2022_01] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2022_02] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2023_01] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
    UNION
    SELECT CUST_ACCOUNT_ID, PARTY_NAME FROM dbo.[2023_02] WHERE CUST_ACCOUNT_ID IS NOT NULL AND PARTY_NAME IS NOT NULL
   ) AS CombinedData;

-- Inserción en D_product
--DELETE FROM  dbo.[2019_01] WHERE SEGMENT1 is NULL
---conversion de float a varchar-----------------------------------
--ALTER TABLE dbo.[2019_01] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2019_02] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2020_01] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2020_02] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2021_01] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2021_02] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2022_01] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2022_02] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2023_01] ALTER COLUMN SEGMENT1 VARCHAR(150);
--ALTER TABLE dbo.[2023_02] ALTER COLUMN SEGMENT1 VARCHAR(150);
-----------------------------------------------------------
INSERT INTO D_product (SEGMENT1, DESCRIPTION_PRODUCT)
SELECT DISTINCT SEGMENT1,DESCRIPTION
FROM
   (
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2019_01] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2019_02] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2020_01] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2020_02] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2021_01] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2021_02] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2022_01] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2022_02] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2023_01] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
    UNION
    SELECT SEGMENT1, DESCRIPTION FROM dbo.[2023_02] WHERE SEGMENT1 IS NOT NULL AND DESCRIPTION IS NOT NULL
   ) AS CombinedData;

-- Inserción en D_fecha
INSERT INTO D_fecha (TRX_DATE, Month)
SELECT DISTINCT
    TRX_DATE,
    MONTH(TRX_DATE) AS Month
FROM
   (
    SELECT TRX_DATE FROM dbo.[2019_01] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2019_02] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2020_01] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2020_02] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2021_01] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2021_02] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2022_01] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2022_02] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2023_01] WHERE TRX_DATE IS NOT NULL
    UNION
    SELECT TRX_DATE FROM dbo.[2023_02] WHERE TRX_DATE IS NOT NULL
   ) AS CombinedData;


-- Inserción en D_canal
-- Insertar canales en D_canal para todos los años
INSERT INTO D_canal (SALES_CHANNEL_CODE)
SELECT DISTINCT SALES_CHANNEL_CODE
FROM
   (
    SELECT SALES_CHANNEL_CODE FROM dbo.[2019_01] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2019_02] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2020_01] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2020_02] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2021_01] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2021_02] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2022_01] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2022_02] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2023_01] WHERE SALES_CHANNEL_CODE IS NOT NULL
    UNION
    SELECT SALES_CHANNEL_CODE FROM dbo.[2023_02] WHERE SALES_CHANNEL_CODE IS NOT NULL
   ) AS CombinedData;


-- Inserción en Fact_ventas
-- Inserción de datos en Fact_ventas para el año 2019_01
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2019_01].QUANTITY_INVOICED,
    [dbo].[2019_01].EXTENDED_AMOUNT
FROM
    [dbo].[2019_01]
    INNER JOIN D_fecha T ON [dbo].[2019_01].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2019_01].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2019_01].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2019_01].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2019_02].QUANTITY_INVOICED,
    [dbo].[2019_02].EXTENDED_AMOUNT
FROM
    [dbo].[2019_02]
    INNER JOIN D_fecha T ON [dbo].[2019_02].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2019_02].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2019_02].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2019_02].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

	INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2020_01].QUANTITY_INVOICED,
    [dbo].[2020_01].EXTENDED_AMOUNT
FROM
    [dbo].[2020_01]
    INNER JOIN D_fecha T ON [dbo].[2020_01].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2020_01].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2020_01].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2020_01].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2020_02
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2020_02].QUANTITY_INVOICED,
    [dbo].[2020_02].EXTENDED_AMOUNT
FROM
    [dbo].[2020_02]
    INNER JOIN D_fecha T ON [dbo].[2020_02].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2020_02].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2020_02].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2020_02].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2021_01
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2021_01].QUANTITY_INVOICED,
    [dbo].[2021_01].EXTENDED_AMOUNT
FROM
    [dbo].[2021_01]
    INNER JOIN D_fecha T ON [dbo].[2021_01].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2021_01].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2021_01].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2021_01].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2021_02
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2021_02].QUANTITY_INVOICED,
    [dbo].[2021_02].EXTENDED_AMOUNT
FROM
    [dbo].[2021_02]
    INNER JOIN D_fecha T ON [dbo].[2021_02].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2021_02].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2021_02].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2021_02].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2022_01
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2022_01].QUANTITY_INVOICED,
    [dbo].[2022_01].EXTENDED_AMOUNT
FROM
    [dbo].[2022_01]
    INNER JOIN D_fecha T ON [dbo].[2022_01].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2022_01].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2022_01].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2022_01].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2022_02
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2022_02].QUANTITY_INVOICED,
    [dbo].[2022_02].EXTENDED_AMOUNT
FROM
    [dbo].[2022_02]
    INNER JOIN D_fecha T ON [dbo].[2022_02].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2022_02].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2022_02].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2022_02].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

-- Para 2023_01
INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2023_01].QUANTITY_INVOICED,
    [dbo].[2023_01].EXTENDED_AMOUNT
FROM
    [dbo].[2023_01]
    INNER JOIN D_fecha T ON [dbo].[2023_01].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2023_01].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2023_01].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2023_01].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

INSERT INTO Fact_ventas (CUST_ACCOUNT_ID, TRX_DATE, SEGMENT1, SALES_CHANNEL_CODE, QUANTITY_INVOICED, EXTENDED_AMOUNT)
SELECT
    TC.CUST_ACCOUNT_ID,
    T.TRX_DATE,
    P.SEGMENT1,
    C.SALES_CHANNEL_CODE,
    [dbo].[2023_02].QUANTITY_INVOICED,
    [dbo].[2023_02].EXTENDED_AMOUNT
FROM
    [dbo].[2023_02]
    INNER JOIN D_fecha T ON [dbo].[2023_02].TRX_DATE = T.TRX_DATE
    INNER JOIN D_product P ON [dbo].[2023_02].SEGMENT1 = P.SEGMENT1
    INNER JOIN D_canal C ON [dbo].[2023_02].SALES_CHANNEL_CODE = C.SALES_CHANNEL_CODE
    INNER JOIN D_clientes TC ON [dbo].[2023_02].CUST_ACCOUNT_ID = TC.CUST_ACCOUNT_ID;

