import pandas as pd
from dotenv import load_dotenv
from get_cake_zips import get_zips_from_cake
import sys

if len(sys.argv) < 2:
    print("Please provide a vertical to check")
    sys.exit(1)
vertical_id=sys.argv[1]
contract_status=1
# Function to validate zip codes
def validate_zip_codes(zipcodes):
    invalid_zipcodes = pd.DataFrame(columns=['Zip', 'Buyer ID', 'Contract ID'])
    invalid_rows = []

    for _, row in zipcodes.iterrows():
        zipcode = row['Zip']
        if not zipcode.isdigit() or len(zipcode) != 5:
            invalid_rows.append({
                'Zip': zipcode,
                'Buyer ID': row['Buyer ID'],
                'Buyer Name': row['Buyer Name'],
                'Contract ID': row['Contract ID']
            })
    
    if invalid_rows:
        invalid_zipcodes = pd.concat([invalid_zipcodes, pd.DataFrame(invalid_rows)], ignore_index=True)

    return invalid_zipcodes

def main():
    raw_zip_df_active=get_zips_from_cake(0,0,vertical_id,1)
    raw_zip_df_pending=get_zips_from_cake(0,0,vertical_id,3)
    raw_zip_df=pd.concat([raw_zip_df_active,raw_zip_df_pending],ignore_index=True)
    zipcodes = pd.DataFrame(raw_zip_df)
    zipcodes['Zip'] = zipcodes['Zip Code'].astype(str)
    print(zipcodes)
    invalid_zipcodes = validate_zip_codes(zipcodes)
    if not invalid_zipcodes.empty:
        print("Invalid zip codes found:")
        invalid_zipcodes.to_csv(f'invalid_zipcodes_{vertical_id}.csv', index=False)
        #for _, row in invalid_zipcodes.iterrows():
         #   print(f"Zip: {row['Zip']}, Buyer ID: {row['Buyer ID']}, Contract ID: {row['Contract ID']}")
        print("Please fix the invalid zip codes and try again.")
        exit(1)  # Exit the program with an error code
    else:
        print("All zip codes are valid.")
if __name__ == "__main__":
    main()
