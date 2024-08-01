import os
from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
load_dotenv()
import pandas as pd

api_key=os.getenv("API_POST_KEY")
get_contract_base_url = "https://track.epathmedia.com/api/4/EXPORT.asmx/BuyerContracts"
get_buyers_base_url = "https://track.epathmedia.com/api/2/export.asmx/Buyers"

def cake_get_request(api_url, params):
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def fetch_data_from_api(api_url,buyer_id,contract_id, vertical_id, contract_status):
    params = {
        "api_key": api_key,
        "buyer_id": buyer_id,
        "buyer_contract_id": contract_id,
        "vertical_id": vertical_id,
        "buyer_contract_status_id":contract_status
    }
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
    df = pd.DataFrame(zip_codes, columns=['Zip Code'])
    return df

def parse_buyer_xml_response(xml_content):
    namespaces = {'ns': 'http://cakemarketing.com/api/2/'}
    root = ET.fromstring(xml_content)
    buyer_elements = root.findall('.//ns:buyer', namespaces)
    buyers = []
    for buyer_element in buyer_elements:
        buyer_id = buyer_element.find('ns:buyer_id', namespaces).text
        buyers.append(buyer_id)
    return buyers

def parse_contract_xml_response_multiple(xml_content):
    namespaces = {
        'ns': 'http://cakemarketing.com/api/4/',
        'API': 'API:id_name_store'
    }
    root = ET.fromstring(xml_content)
    buyer_contracts = root.findall('.//ns:buyer_contract', namespaces)
    
    data = []
    for contract in buyer_contracts:
        contract_id = contract.find('ns:buyer_contract_id', namespaces).text
        buyer_element = contract.find('.//ns:buyer', namespaces)
        buyer_id = buyer_element.find('API:buyer_id', namespaces).text if buyer_element is not None else None
        buyer_name = buyer_element.find('API:buyer_name', namespaces).text if buyer_element is not None else None
        filter_elements = contract.findall('.//ns:filters/ns:filter', namespaces)
        zip_codes = []
        for filter_element in filter_elements:
            filter_type_name = filter_element.find('ns:filter_type/ns:filter_type_name', namespaces).text
            if filter_type_name == "Zip code contains":
                param_string = filter_element.find('ns:param_string', namespaces).text
                zip_codes.extend(param_string.split('|'))
        data.extend([(contract_id, buyer_id, buyer_name, zipcode) for zipcode in zip_codes])
    
    df = pd.DataFrame(data, columns=['Contract ID', 'Buyer ID', 'Buyer Name','Zip Code'])
    return df

def write_file(df,buyer_id,contract_id):
    csv_file_path="Cake_zips_buyer_"+str(buyer_id)+"_contract_"+str(contract_id)+".csv"
    df.to_csv(csv_file_path, index=False)
    #print(df)

def output_csv_files(df, output_dir):
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Iterate through each unique combination of buyer_id and contract_id
    for buyer_id in df['Buyer ID'].unique():
        buyer_df = df[df['Buyer ID'] == buyer_id]
        buyer_dir = os.path.join(output_dir, str(buyer_id))
        if not os.path.exists(buyer_dir):
            os.makedirs(buyer_dir)
        
        for contract_id in buyer_df['Contract ID'].unique():
            contract_df = buyer_df[buyer_df['Contract ID'] == contract_id]
            contract_dir = os.path.join(buyer_dir, str(contract_id))
            if not os.path.exists(contract_dir):
                os.makedirs(contract_dir)

            # Extract zip codes for the current buyer_id and contract_id
            zips = contract_df['Zip Code']

            # Write zips to CSV file
            csv_file = os.path.join(contract_dir, 'zips.csv')
            with open(csv_file, 'w') as f:
                f.write('Zip Code\n')
                for zip_code in zips:
                    f.write(f'{zip_code}\n')
#1 = active   | 2 = inactive | 3 = Pending
def get_zips_from_cake(buyer_id,contract_id,vertical_id,contract_status=1,output_type="df"):
    contract_params = {
        "api_key": api_key,
        "buyer_id": buyer_id,
        "buyer_contract_id": contract_id,
        "vertical_id": vertical_id,
        "buyer_contract_status_id":contract_status
    }
    buyer_params={
        "api_key": api_key,
        "buyer_id": 0,
        "account_status_id": 1
    }
    active_buyers_xml_content=cake_get_request(get_buyers_base_url, buyer_params)
    active_buyers=parse_buyer_xml_response(active_buyers_xml_content)
    contract_xml_content = cake_get_request(get_contract_base_url, contract_params)
    
    df_all =parse_contract_xml_response_multiple(contract_xml_content)
    
    df_all = df_all[df_all['Buyer ID'].isin(active_buyers)]

    if output_type=="csv":
        individual_files_directory="cake_active_buyer_zips/"+str(vertical_id)
        combined_file_path="Cake_active_buyer_zips_"+str(vertical_id)+".csv"
        output_csv_files(df_all, individual_files_directory)
        df_all.to_csv(combined_file_path, index=False)
    else:
        return df_all
    
class CakeZips:
    def __init__(self,buyer_id,contract_id,vertical_id,contract_status=1,output_type="df"):
        self.buyer_id=buyer_id
        self.contract_id=contract_id
        self.vertical_id=vertical_id
        self.contract_status=contract_status
        self.output_type=output_type
        self.df_all=get_zips_from_cake(self.buyer_id,self.contract_id,self.vertical_id,self.contract_status,self.output_type)
        if self.output_type=="df":
            print(self.df_all)
        else:
            print("Files have been created")

def main():
    vertical="135"
    get_zips_from_cake(0,0,vertical,1,"csv")
if __name__ == "__main__":
    main()

