import requests
from src.logging.logger import logging
import pandas as pd
import os
from dotenv import load_dotenv 
load_dotenv()

def fetch_crypto_data(api_uri): 
    response = requests.get(
        api_uri,
        params={
            "market": "cadli",
            "instrument": "BTC-USD",
            "limit": 5000,
            "aggregate": 1,
            "fill": "true",
            "apply_mapping": "true",
            "response_format": "JSON"
        },
        headers={"Content-type": "application/json; charset=UTF-8"}
    )

    if response.status_code == 200:
        logging.info('API Connection Successful! \nFetching the data...') 
        data = response.json()
        data_list = data.get('Data', []) 
        df = pd.DataFrame(data_list) 
        df['DATE'] = pd.to_datetime(df['TIMESTAMP'], unit='s') 
        logging.info(f"df.head : {df.head()}")
        logging.info(f"df.tail: {df.tail()}")
        logging.info(f"df length: {len(df)}")
        return df 
    else:
        raise Exception(f"API Error: {response.status_code} - {response.text}")


if __name__ == "__main__":
    API_URI = os.getenv("API_URI")
    df = fetch_crypto_data(api_uri = API_URI)
    logging.info(f"df.head : {df.head()}")
    logging.info(f"df.tail: {df.tail()}")
    logging.info(f"df length: {len(df)}")