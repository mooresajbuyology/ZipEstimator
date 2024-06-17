
import pandas as pd
import math
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from get_cake_zips import get_zips_from_cake
load_dotenv()

# Get user input uncomment to use and comment hard coded values
# target_price = float(input("Enter the price the new buyer plans to pay: "))
# zipcode_data = str(input("Enter path to buyer zips csv: "))
# skip_lines = int(input("Number of header rows (0 if none): "))
# sales_data = str(input("Enter path to master leads csv: "))
#

zipcode_data = 'buyer_zips/buyer_zips.csv'
target_price = float(65)
skip_lines=1
include_poor=False
include_fair=False
include_Good=True
include_Excellent=True
include_phone_no_match_not_found=True
safety_only=False
vertical_id=134
start_date="2024-05-31"
end_date="2024-06-13"
#bathroom 134 tubs 94
use_csv_file_for_raw_leads=False
sales_data = 'Raw_leads/bath_March_2024.csv'

#additional parameters to consider tweaking
buyer_price_increments=int(5)
#win_below= round((target_price * 0.7),0)
win_below = math.floor(target_price * 0.5 / buyer_price_increments) * buyer_price_increments
ignore_sweeper_buyers=False
ignore_buyer_percent=float(0.15)

#key variables hard coded
credit_rating_Poor_exclude_value = ["Poor"]
credit_rating_Fair_exclude_value = ["Fair"]
credit_rating_Good_exclude_value = ["Good"]
credit_rating_Excellent_exclude_value = ["Excellent"]
phone_no_match_not_found_array=["no-match","not-found"]
anura_bad=["bad"]
interested_in_safety_array=["Yes","yes"]
# Define excluded buyer contracts ##future optimizaiton put this in a seperate file or table 
# removed for now('Optmze - Tubs - Aff Poor', 'Optmze - Tubs - Claritas','Optmze - Tubs - Fair/ Poor',)
excluded_buyers = ['InternalAff - Tubs', 'QC - Tubs - bad', 'InternalAff - Bathroom','DNC - Bathroom','QC - Tub Liners - bad']
sweeper_buyers =['Quinstreet','Contractor Appointments']
production_engine =create_engine(os.getenv("PROD_ENGINE"))
# more work to match to db names and tables 
raw_leads_query =f"""
SELECT
  leads.unique_id as "Lead ID",
  postal_code as "Zip",
  buyer_leads.buyer_contract_id as "Contract_id",
  buyer_leads.buyer_contract_name as "Buyer Contract",
  buyer_leads.buyer_name as "Buyer Name",
  buyer_leads.unique_lead_id,
  buyer_leads.price_amount "Price",
  COALESCE(leads.data -> 'walk_in_tubs_anura'::text, leads.data -> 'bathroom_remodel_anura'::text) AS "Anura",
  COALESCE(leads.data -> 'walk_in_tubs_phone_subscriber_name'::text, leads.data -> 'bathroom_remodel_phone_subscriber_name'::text) AS "Phone Subscriber Name",
  COALESCE(leads.data -> 'walk_in_tubs_creditrating'::text,leads.data -> 'bathroom_remodel_creditrating'::text)AS "Credit Rating",
  COALESCE(leads.data -> 'walk_in_tubs_safetyfeatures'::text, leads.data -> 'bathroom_remodel_safetyproducts'::text) AS "Interested in Safety Products"
FROM buyer_leads
     JOIN leads ON buyer_leads.unique_lead_id::text = leads.unique_id::text
WHERE
    leads.vertical_id={vertical_id}
    AND
    buyer_leads.transaction_date BETWEEN '{start_date}' AND '{end_date}' ;
"""

if use_csv_file_for_raw_leads==True:
    # Read CSV files
    sales = pd.read_csv(sales_data, dtype={'Zip': str})
else:
    sales=pd.read_sql(raw_leads_query, production_engine, dtype={'Zip': str})

# Function to read file based on file extension
zipcodes =[]

if zipcode_data.endswith('.csv'):
    zipcodes = pd.read_csv(zipcode_data,usecols=[0],header=None,skiprows=skip_lines,names=['Zip'], dtype={'Zip': str})
elif zipcode_data.endswith('.xlsx') or zipcode_data.endswith('.xls'):
    zipcodes = pd.read_excel(zipcode_data,usecols=[0],header=None,skiprows=skip_lines,names=['Zip'], dtype={'Zip': str})
else:
    raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")


# Function to validate zip codes
def validate_zip_codes(zipcodes):
    invalid_zipcodes = []
    for zipcode in zipcodes['Zip']:
        if not zipcode.isdigit() or len(zipcode) != 5:
            invalid_zipcodes.append(zipcode)
    return invalid_zipcodes

#filters out excluded buyer contracts and bad credit
def filter_sales_data(sales):
    # Filter out excluded buyers and anura bad
    sales = sales[~sales['Buyer Contract'].isin(excluded_buyers)]
    sales = sales[~sales['Anura'].isin(anura_bad)]
   
      # Filter out credit
    if include_poor==False:
        sales = sales[~sales['Credit Rating'].isin(credit_rating_Poor_exclude_value)]
    if include_fair==False:
        sales = sales[~sales['Credit Rating'].isin(credit_rating_Fair_exclude_value)]
    if include_Good==False:
        sales = sales[~sales['Credit Rating'].isin(credit_rating_Good_exclude_value)]
    if include_Excellent==False:
        sales = sales[~sales['Credit Rating'].isin(credit_rating_Excellent_exclude_value)]
    if include_phone_no_match_not_found==False:
        sales = sales[~sales['Phone Subscriber Name'].isin(phone_no_match_not_found_array)]
    if safety_only==True:
        sales = sales[~sales['Interested in Safety Products'].isin(interested_in_safety_array)]
    return sales

#remove $ from price field
def clean_price(price):
    # Remove "$" and convert to float
    return float(price.replace('$', ''))


# Function to calculate sales below a given price
def calculate_sales(merged_data,target_price):
    available_sales = merged_data[merged_data['Price'] <= target_price]['Price'].count()
    return available_sales
# Function to calculate average
def calculate_average(merged_data,max_price,include_0):
    if include_0==False :
        average_price = merged_data[(merged_data['Price'] <= max_price) & (0 < merged_data['Price'])]['Price'].mean()
    else:
        average_price = merged_data[merged_data['Price'] <= max_price]['Price'].mean()
    return average_price

#finds # of buyers in a price range given a data set
def calculate_buyers_in_range(low_price,high_price,sales,return_name=False):
    price_filtered_data = sales[(low_price < sales['Price']) & (sales['Price'] <= high_price)]
    # Calculate total number of leads in the price range
    total_leads = price_filtered_data['Lead ID'].nunique()

    # Calculate number of leads purchased by each buyer 
    buyer_lead_counts = price_filtered_data.groupby('Buyer Name')['Lead ID'].nunique().reset_index()
    buyer_lead_counts.rename(columns={'Lead ID': 'Buyer Leads'}, inplace=True)
    
    # Filter buyers who purchased more than  ignore_buyer_percent % of the leads in the range
    qualified_buyers = buyer_lead_counts[buyer_lead_counts['Buyer Leads'] > ignore_buyer_percent * total_leads]

    #Remove sweeperbuyers
    if ignore_sweeper_buyers==True:
        qualified_buyers  = qualified_buyers[~qualified_buyers['Buyer Name'].isin(sweeper_buyers)]

    # Count the number of qualified buyers
    qualified_buyer_count = qualified_buyers.shape[0]
    if return_name==True:
        return qualified_buyers['Buyer Name'].tolist()
    else:
        return qualified_buyer_count
def calculate_leads_by_buyers(sales, buyers):
    buyer_filtered_data = sales[sales['Buyer Name'].isin(buyers)]
    # Calculate total number of leads in the price range
    total_leads = buyer_filtered_data['Lead ID'].nunique()

    return total_leads

#finds number of leads in a price range given a data set
def calculate_leads_in_range(low_price,high_price,sales):
    price_filered_data = sales[(low_price < sales['Price']) & (sales['Price'] <= high_price)]
    lead_count = price_filered_data['Lead ID'].nunique()
    return lead_count

# Check for invalid zip codes
invalid_zipcodes = validate_zip_codes(zipcodes)
if invalid_zipcodes:
    print("Invalid zip codes found:")
    for invalid_zipcode in invalid_zipcodes:
        print(invalid_zipcode)
    print("Please fix the invalid zip codes and try again.")
    exit(1)  # Exit the program with an error code

original_count_zips = len(zipcodes)
zipcodes.dropna(subset=['Zip'], inplace=True)  # Remove blank zipcodes
zip_counts_no_blank= len(zipcodes)
zipcodes.drop_duplicates(subset=['Zip'], inplace=True)  # Remove duplicate zipcodes
final_count_zips = len(zipcodes)
if original_count_zips != final_count_zips:
    print(f"Warning: {original_count_zips - zip_counts_no_blank} blank and { zip_counts_no_blank-final_count_zips} duplicate zip codes were removed.")

#clean data, merge and get totals
if use_csv_file_for_raw_leads==True:
    sales['Price'] = sales['Price'].apply(clean_price)

merged_data = pd.merge(sales, zipcodes, on='Zip', how='inner')

unfiltered_sales_in_zips = calculate_sales(merged_data, target_price)

# Calculate filtered sales
sales = filter_sales_data(merged_data)

total_average =calculate_average(sales,500,True)
target_average=calculate_average(sales,target_price,False)
print("average price in zips:",total_average)
print("average price below target:",target_average)
sales_in_zips = calculate_sales(sales, target_price)
print("Total sales in zips below price point: ",unfiltered_sales_in_zips)
print("Filtered sales in zips: ",sales_in_zips)

# Iterate through price ranges in 5-dollar increments
total_leads_estimate_upperend = 0
for price_range in range(int(win_below), int(target_price), buyer_price_increments):
    range_low = price_range
    range_high = price_range + buyer_price_increments

    # Ensure range_high does not exceed target_price
    if range_high > target_price:
        range_high = target_price

    leads_in_current_range=calculate_leads_in_range(range_low,range_high,sales)
    buyers_count_in_current_range=calculate_buyers_in_range(range_low,range_high,sales)
    buyers_names_in_current_range = calculate_buyers_in_range(range_low, range_high, sales, return_name=True)
    buyers_names_in_current_range = ', '.join(buyers_names_in_current_range)
    expected_win_share_in_current_range=leads_in_current_range/(buyers_count_in_current_range+1)
    total_leads_estimate_upperend+=expected_win_share_in_current_range

    print(f"Price Range: {range_low} - {range_high}, Buyers: {buyers_count_in_current_range} ({buyers_names_in_current_range}), Leads:{leads_in_current_range}, Expected share:{round(expected_win_share_in_current_range,2)}")
    #print(f"Buyer Names:{buyers_names_in_current_range}")

#get all Buyers in uper range
total_buyers_upperend=calculate_buyers_in_range(win_below,target_price,sales)
total_buyers_lowerend=calculate_buyers_in_range(-1,(win_below),sales)
buyer_names_lowerend=calculate_buyers_in_range(-1,(win_below),sales,return_name=True)
total_leads_upperend=calculate_leads_in_range(win_below,target_price,sales)
total_leads_lowerend=calculate_leads_in_range(-1,(win_below),sales)

# Display the result

#print("Upper end",win_below," - ", target_price," Buyers:",total_buyers_upperend," Leads:",total_leads_upperend, "Estimated share: ", round(total_leads_estimate_upperend,2))
print(f"Lower end:  0 - {win_below}, Buyers: {total_buyers_lowerend} ({buyer_names_lowerend}), Leads: {total_leads_lowerend} (Sold to ourselves: {calculate_leads_by_buyers(sales, ['BuyologyIQ'])}), Estimated share: {total_leads_lowerend}")
print(f"Expected total lead share {round((total_leads_estimate_upperend+total_leads_lowerend),2)}")

