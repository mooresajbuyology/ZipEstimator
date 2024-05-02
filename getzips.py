import os
from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
import json
load_dotenv()
import html
import logging
import pandas as pd

api_key=os.getenv("API_POST_KEY")
buyer_id=750
contract_id=1704
vertical_id=134

 # File to store last checked timestamp
last_checked_file = "Processing_Files/last_checked.txt"
processed_leads_file = "Processing_Files/processed_leads.txt"

def configure_logging():
    # Configure the root logger to log INFO level and higher to a log file
    logging.basicConfig(filename='Logs/all_logs.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create a separate logger for error level logging
    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)

    # Create a file handler for the error logger
    error_handler = logging.FileHandler('Logs/error_logs.log')
    error_handler.setLevel(logging.ERROR)

    # Define a formatter for the error handler
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)

    # Add the file handler to the error logger
    error_logger.addHandler(error_handler)

def fetch_data_from_api(api_url,params):
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def parse_xml_response(xml_content):
    namespaces = {'ns': 'http://cakemarketing.com/api/4/'}
    root = ET.fromstring(xml_content)
    filter_elements = root.findall('.//ns:filter', namespaces)
    zip_codes = []
    for filter_element in filter_elements:
        filter_type_name = filter_element.find('ns:filter_type/ns:filter_type_name', namespaces).text
        if filter_type_name == "Zip code contains":
            param_string = filter_element.find('ns:param_string', namespaces).text
            zip_codes.extend(param_string.split('|'))
    return zip_codes

def main():
    base_url = "https://track.epathmedia.com/api/4/EXPORT.asmx/BuyerContracts"
    params = {
        "api_key": api_key,
        "buyer_id": buyer_id,
        "buyer_contract_id": contract_id,
        "vertical_id": vertical_id,
        "buyer_contract_status_id":0
    }
    xml_content = fetch_data_from_api(base_url, params)
    zip_codes = parse_xml_response(xml_content)

    # Create a DataFrame from the extracted zip codes
    df = pd.DataFrame({'Zip Codes': zip_codes})
    csv_file_path="Cake_zips_buyer_"+str(buyer_id)+"_contract_"+str(contract_id)+".csv"
    df.to_csv(csv_file_path, index=False)
    print(df)

if __name__ == "__main__":
    main()