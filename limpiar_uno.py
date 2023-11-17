import pandas as pd
import chardet

# Function to detect the encoding of a file
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# File path
file_path = r'D:\UsX\Escritorio\ETL_python\2019_01.csv'

# Detect encoding
encoding = detect_encoding(file_path)

# Read CSV with detected encoding
df = pd.read_csv(file_path, encoding=encoding)

# Convert only string columns to UTF-8 encoding
df = df.apply(lambda x: x.str.encode('utf-8').str.decode('utf-8') if x.dtype == 'O' else x)

# Data cleaning steps
columns_to_drop = ['SOURCE_NAME', 'CUST_ACCOUNT_ID', 'SITE_USE_CODE', 'SITE_SHIP_TO',
                   'LOCATION_SHIP_TO', 'WAREHOUSE_ID', 'TAX_RATE', 'ORG_ID', 'INTERFACE_HEADER_ATTRIBUTE2',
                   'CUST_TRX_TYPE_ID', 'NAME', 'TYPE', 'PARTY_SITE_ID', 'SEGMENT4', 'X']
df = df.drop(columns=columns_to_drop, axis=1)

# ... other data cleaning steps ...

# Save the cleaned DataFrame to a new CSV file with UTF-8 encoding
output_file_path = r'D:\UsX\Escritorio\ETL_python\cleaned_file.csv'
df.to_csv(output_file_path, index=False, encoding='utf-8')
