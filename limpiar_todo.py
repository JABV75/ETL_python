#Aquí se importan las librerías a utilizar
import pandas as pd
import chardet

#En esta sección se crea la función para detectar la codificación de los caracteres en los datos
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

#En esta sección se especifica el nombre de los archivos a leer

x = ['2019_01', '2019_02', '2020_01', '2020_02', '2021_01', '2021_02', '2022_01', '2022_02', '2023_01', '2023_02']

#En esta sección se crea un DataFrame vacío para concatenar los datos en caso de utilizarse

chunk_size = 90000

#En esta sección se itera sobre los archivos a leer
for i in x:
    
    #open the file in windows
    #file_path = 'D:\\UsX\\Escritorio\\data\\' + i + '.csv'
    
    #open the file in mac
    file_path = '/Users/josebenvenuto/Desktop/data/' + i + '.csv'
    
    print(i)
    
    #Aqui se detecta la codificación de los caracteres en los datos
    encoding = detect_encoding(file_path)

    # Leer el archivo en chunks
    chunks = pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size, low_memory=False)

    # Aqui se itera sobre los chunks para limpiar los datos
    for chunk in chunks:

        
        # Convert only string columns to UTF-8 encoding
        chunk = chunk.apply(lambda x: x.str.encode('utf-8').str.decode('utf-8') if x.dtype == 'O' else x)

        # Aqui se eliminan las columnas que no se utilizarán
        chunk = chunk.drop(['SOURCE_NAME', 'SITE_USE_CODE', 'LOCATION', 'TAX_CODE', 'SITE_SHIP_TO', 'LOCATION_SHIP_TO',
                            'TAX_SHIP_TO', 'ATTRIBUTE10', 'WAREHOUSE_ID', 'TRX_NUMBER', 'LINE_NUMBER', 'SALES_ORDER', 'SALES_ORDER_LINE', 'SALES_ORDER_SOURCE',
                            'TAX_RATE', 'ORG_ID', 'INTERFACE_HEADER_ATTRIBUTE2', 'CUST_TRX_TYPE_ID', 'NAME', 'TYPE', 'INVOICE_CURRENCY_CODE', 'INVENTORY_ITEM_ID',
                            'GL_DATE', 'TAX_CLASSIFICATION_CODE', 'QUANTITY_CREDITED', 'UNIT_SELLING_PRICE', 'EXCHANGE_RATE', 'CUSTOMER_TRX_ID',
                            'CUSTOMER_TRX_LINE_ID', 'PARTY_SITE_ID', 'SEGMENT4', 'X'], axis=1)

        # Aqui se cambian los tipos de datos de las columnas
        chunk['CUST_ACCOUNT_ID'] = pd.to_numeric(chunk['CUST_ACCOUNT_ID'], errors='coerce').astype('Int64')
        chunk['PARTY_NAME'] = chunk['PARTY_NAME'].astype(str)
        chunk['SALES_CHANNEL_CODE'] = chunk['SALES_CHANNEL_CODE'].astype(str)
        chunk['SEGMENT1'] = pd.to_numeric(chunk['SEGMENT1'], errors='coerce').astype('Int64')
        chunk['DESCRIPTION'] = chunk['DESCRIPTION'].astype(str)
        chunk['TRX_DATE'] = pd.to_datetime(chunk['TRX_DATE'], format='%d-%b-%Y %H:%M:%S', errors='coerce').dt.date
        #chunk['TRX_DATE'] = pd.to_datetime(chunk['TRX_DATE'], format='%d-%b-%Y %H:%M:%S', errors='coerce').dt.strftime('%Y/%m/%d')
        chunk['QUANTITY_INVOICED'] = pd.to_numeric(chunk['QUANTITY_INVOICED'], errors='coerce').astype('float64')
        chunk['EXTENDED_AMOUNT'] = pd.to_numeric(chunk['EXTENDED_AMOUNT'], errors='coerce').astype('float64')

        #save the chunk in a new csv in windows
        #chunk.to_csv('D:\\UsX\\Escritorio\\data\\' + i + '_clean.csv', index=False, encoding='utf-8')
        
        #save the chunk in a new csv in mac
        chunk.to_csv('/Users/josebenvenuto/Desktop/data/' + i + '_clean.csv', index=False, encoding='utf-8')
