#!pip install pandas==1.3.3 -y
#!pip install requests==2.26.0 -y

import glob
import pandas as pd
from datetime import datetime

#data
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv

#JSON extract
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

#extract function
def extract():
    extracted_data= pd.read_json('bank_market_cap_1.json')
    return extracted_data

df = pd.read_csv('exchange_rates.csv', index_col=0)
val = df[df.index.str.startswith('GBP')]
exchange_rate = val.iloc[0]['Rates']
exchange_rate

#transform function
def transform(data):
    data['Market Cap (US$ Billion)'] = (data['Market Cap (US$ Billion)'] * exchange_rate).round(decimals=3)
    data.rename(columns={'Market Cap (US$ Billion)':'Market Cap (GBP$ Billion)'}, inplace=True)
    return data

#load function
def load(data_to_load):
    data_to_load.to_csv('bank_market_cap_gbp.csv', index=False)

#logging function
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

#Run ETL process
log("ETL Job Started")
log("Extract phase Started")
# Call the extract function
extracted_data = extract()
# Print the rows
extracted_data.head(5)
log("Extract phase Ended")

log("Transform phase Started")
# Call the transform function
transformed_data = transform(extracted_data)
# Print the first 5 rows
transformed_data.head(5)
log("Transform phase Ended")

log("Load phase Started")
#Call the load function
load(transformed_data)
log("Load phase Ended")