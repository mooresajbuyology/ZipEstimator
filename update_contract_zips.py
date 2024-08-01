import requests
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv
import pandas as pd
import xmltodict
from pandas import json_normalize
load_dotenv()


api_key=os.getenv("API_POST_KEY")
get_contract_base_url = "https://track.epathmedia.com/api/4/EXPORT.asmx/BuyerContracts"
buyer_id = 0
contract_id= 1631
vertical_id = 0
contract_status = 0

def cake_get_api_request(api_url,params,headers=None):
    response = requests.get(api_url, params=params,headers=headers)
    try:
        response_xml = ET.fromstring(response.content)
        success_element = response_xml.find('.//{http://cakemarketing.com/api/1/}success')
        if response.status_code != 200 or success_element is None or success_element.text.lower() == 'false':
            error_message = response.text if success_element is None else f"<success>{success_element.text}</success>"
            print(f"Error occurred getting leads in range from API: {error_message}")
            print("Request:")
            print(api_url)
            print("Response:")
            print(response.content)
        else:
            print(f"Lead Query successful")
    except ET.ParseError:
        print("Error parsing XML response")
        return None
    return response.content

contract_params = {
        "api_key": api_key,
        "buyer_id": buyer_id,
        "buyer_contract_id": contract_id,
        "vertical_id": vertical_id,
        "buyer_contract_status_id":contract_status
    }
xml=cake_get_api_request(get_contract_base_url,contract_params)
data_dict = xmltodict.parse(xml)
flat_dict = json_normalize(data_dict)
df=pd.DataFrame(flat_dict)
print(df.columns)