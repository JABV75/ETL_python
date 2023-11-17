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

# Loop through each file
for i in x:
    file_path = 'D:\\UsX\\Escritorio\\ETL_python\\' + i + '.csv'

    # Detect encoding
    encoding = detect_encoding(file_path)

    # Read CSV with detected encoding
    df = pd.read_csv(file_path, encoding=encoding, low_memory=False)

    # Convert only string columns to UTF-8 encoding
    df = df.apply(lambda x: x.str.encode('utf-8').str.decode('utf-8') if x.dtype == 'O' else x)

    # Continue with your data cleaning and manipulation steps
    # df = df.dropna()
    # df = df.drop_duplicates()
    df = df.drop(['SOURCE_NAME', 'CUST_ACCOUNT_ID', 'SITE_USE_CODE', 'SITE_SHIP_TO',
                   'LOCATION_SHIP_TO', 'WAREHOUSE_ID', 'TAX_RATE', 'ORG_ID', 'INTERFACE_HEADER_ATTRIBUTE2',
                   'CUST_TRX_TYPE_ID', 'NAME', 'TYPE', 'PARTY_SITE_ID', 'SEGMENT4', 'X'], axis=1)

    # Concatenate the current DataFrame with the previous ones
    all_data = pd.concat([all_data, df], ignore_index=True)

# ... other data cleaning steps ...

# Save the concatenated and cleaned DataFrame to a new CSV file with UTF-8 encoding
all_data.to_csv('D:\\UsX\\Escritorio\\ETL_python\\cleaned_file.csv', index=False, encoding='utf-8')
