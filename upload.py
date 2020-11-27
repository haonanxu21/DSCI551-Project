

import pandas as pd
import json
import requests

BusinessAnalyst_df = pd.read_csv("BusinessAnalyst.csv")
DataAnalyst_df = pd.read_csv("DataAnalyst.csv")
DataEngineer_df = pd.read_csv("DataEngineer.csv")
##############################################################
BusinessAnalyst_df = BusinessAnalyst_df.drop(columns=['Competitors', 'Easy Apply','Industry'])
DataAnalyst_df = DataAnalyst_df.drop(columns=['Competitors', 'Easy Apply','Industry'])
DataEngineer_df = DataEngineer_df.drop(columns=['Competitors', 'Easy Apply','Industry'])
#############################################################
BusinessAnalyst_df = BusinessAnalyst_df.drop(BusinessAnalyst_df.columns[0], axis=1)
DataAnalyst_df = DataAnalyst_df.drop(DataAnalyst_df.columns[0], axis=1)
DataEngineer_df = DataEngineer_df.drop(DataEngineer_df.columns[0], axis=1)
###############################################################
BA_result = BusinessAnalyst_df.to_json(orient="index")
BA_parsed = json.loads(BA_result)
BA_json_str = json.dumps(BA_parsed)
###############################################################
DA_result = DataAnalyst_df.to_json(orient="index")
DA_parsed = json.loads(DA_result)
DA_json_str = json.dumps(DA_parsed)
###############################################################
DE_result = DataEngineer_df.to_json(orient="index")
DE_parsed = json.loads(DE_result)
DE_json_str = json.dumps(DE_parsed)
##############################################################
url1 = 'https://ds-demo-c0812.firebaseio.com/business_analyst.json'
url2 = 'https://ds-demo-c0812.firebaseio.com/data_analyst.json'
url3 = 'https://ds-demo-c0812.firebaseio.com/data_engineer.json'

response = requests.put(url1, BA_json_str)
response = requests.put(url2, DA_json_str)
response = requests.put(url3, DE_json_str)


#print(DataEngineer_df.columns)
#b_json = BusinessAnalyst_df.to_json()




#with open('data.json', 'w') as f:
#    json.dump(b_json, f)
