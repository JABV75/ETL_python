import pandas as pd
import chardet
import os

# Function to detect file encoding
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# Read in the original CSV file
input_file_path = r'D:\UsX\Escritorio\ETL_python\cleaned_file.csv'
output_folder_path = r'D:\UsX\Escritorio\ETL_python\csvsxcolumnas\\'  # Include a separator at the end
os.makedirs(output_folder_path, exist_ok=True)  # Use os.makedirs to create the folder if it doesn't exist

# Detect encoding
file_encoding = detect_encoding(input_file_path)

# Read CSV with detected encoding
df = pd.read_csv(input_file_path, encoding=file_encoding)

# Loop through each column in the DataFrame
for i, col in enumerate(df.columns):
    # Create a new DataFrame with just the current column
    new_df = pd.DataFrame(df[col])

    # Write the new DataFrame to a CSV file with a name based on the column number
    output_file_path = f'{output_folder_path}c{i+1}.csv'
    new_df.to_csv(output_file_path, index=False, encoding='utf-8')
