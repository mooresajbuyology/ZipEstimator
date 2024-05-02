import pandas as pd
import os

# Example usage:
folder_path = 'combine_files'
output_file = 'combined_data.xlsx'

def combine_files(folder_path, output_file):
    files = os.listdir(folder_path)
    combined_data = pd.DataFrame()

    for file in files:
        file_path = os.path.join(folder_path, file)
        if file.endswith('.csv'):
            data = pd.read_csv(file_path)
        elif file.endswith('.xlsx') or file.endswith('.xls'):
            data = pd.read_excel(file_path)
        else:
            continue
        
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    combined_data.to_excel(output_file, index=False)
    print("Combined file saved successfully.")


combine_files(folder_path, output_file)