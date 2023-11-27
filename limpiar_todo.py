import pandas as pd
import chardet

# Function to detect the encoding of a file
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

x = ['2019_01', '2019_02', '2020_01', '2020_02', '2021_01', '2021_02', '2022_01', '2022_02', '2023_01', '2023_02']

# Initialize an empty DataFrame to store concatenated data
all_data = pd.DataFrame()

# Chunk size for reading CSV files
chunk_size = 50000

# Loop through each file
for i in x:
    file_path = 'D:\\UsX\\Escritorio\\data\\' + i + '.csv'

    # Detect encoding
    encoding = detect_encoding(file_path)

    # Read CSV with detected encoding using chunks
    chunks = pd.read_csv(file_path, encoding=encoding, chunksize=chunk_size, low_memory=False)

    # Process each chunk
    for chunk in chunks:
        # Convert only string columns to UTF-8 encoding
        chunk = chunk.apply(lambda x: x.str.encode('utf-8').str.decode('utf-8') if x.dtype == 'O' else x)

        # Continue with your data cleaning and manipulation steps
        chunk = chunk.drop(['SOURCE_NAME', 'CUST_ACCOUNT_ID', 'SITE_USE_CODE', 'LOCATION', 'TAX_CODE', 'SITE_SHIP_TO',
                            'TAX_SHIP_TO', 'WAREHOUSE_ID', 'LINE_NUMBER', 'SALES_ORDER_SOURCE', 'TAX_RATE', 'ORG_ID',
                            'INTERFACE_HEADER_ATTRIBUTE2', 'CUST_TRX_TYPE_ID', 'NAME', 'TYPE', 'INVENTORY_ITEM_ID',
                            'GL_DATE', 'TAX_CLASSIFICATION_CODE', 'QUANTITY_CREDITED', 'EXCHANGE_RATE',
                            'CUSTOMER_TRX_LINE_ID', 'PARTY_SITE_ID', 'SEGMENT4', 'X'], axis=1)

        # Make the columns of the new CSV cast for each type
        chunk['LOCATION_SHIP_TO'] = chunk['LOCATION_SHIP_TO'].astype(str)
        chunk['PARTY_NAME'] = chunk['PARTY_NAME'].astype(str)
        chunk['SALES_CHANNEL_CODE'] = chunk['SALES_CHANNEL_CODE'].astype(str)
        chunk['ATTRIBUTE10'] = chunk['ATTRIBUTE10'].astype(str)
        chunk['TRX_NUMBER'] = pd.to_numeric(chunk['TRX_NUMBER'], errors='coerce').astype('Int64')
        chunk['SALES_ORDER'] = pd.to_numeric(chunk['SALES_ORDER'], errors='coerce').astype('Int64')
        chunk['SALES_ORDER_LINE'] = pd.to_numeric(chunk['SALES_ORDER_LINE'], errors='coerce').astype('Int64')
        chunk['SEGMENT1'] = pd.to_numeric(chunk['SEGMENT1'], errors='coerce').astype('Int64')
        chunk['DESCRIPTION'] = chunk['DESCRIPTION'].astype(str)
        chunk['TRX_DATE'] = pd.to_datetime(chunk['TRX_DATE'], format='%d-%b-%Y %H:%M:%S', errors='coerce').dt.date
        chunk['INVOICE_CURRENCY_CODE'] = pd.to_numeric(chunk['INVOICE_CURRENCY_CODE'], errors='coerce').astype(str)
        chunk['QUANTITY_INVOICED'] = pd.to_numeric(chunk['QUANTITY_INVOICED'], errors='coerce').astype('float64')
        chunk['UNIT_SELLING_PRICE'] = pd.to_numeric(chunk['UNIT_SELLING_PRICE'], errors='coerce').astype('float64')
        chunk['EXTENDED_AMOUNT'] = pd.to_numeric(chunk['EXTENDED_AMOUNT'], errors='coerce').astype('float64')
        chunk['CUSTOMER_TRX_ID'] = pd.to_numeric(chunk['CUSTOMER_TRX_ID'], errors='coerce').astype('Int64')

        # Concatenate the current chunk with the previous ones
        all_data = pd.concat([all_data, chunk], ignore_index=True)

# ... other data cleaning steps ...

# Save the concatenated and cleaned DataFrame to a new CSV file with UTF-8 encoding
all_data.to_csv('D:\\UsX\\Escritorio\\data\\cleaned_file.csv', index=False, encoding='utf-8')
