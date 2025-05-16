import os
from pymongo import MongoClient
from dotenv import load_dotenv
from crypto_api import fetch_crypto_data   
import pandas as pd 
from src.logging.logger import logging
load_dotenv() 
PYMONGO_URI = os.getenv("PYMONGO_URI")
API_URI = os.getenv("API_URI")

# Connect to the MongoDB client
client = MongoClient(PYMONGO_URI, tls=True)
db = client['bitcoin_crypto_data']
collection = db['bitcoin_historical_data']

try: 
    latest_entry = collection.find_one(sort=[("DATE", -1)])  
    if latest_entry:
        last_date = pd.to_datetime(latest_entry['DATE']).strftime('%Y-%m-%d')
    else:
        last_date = '2011-04-01'  # Default start date if MongoDB is empty

    # Fetch data from the last recorded date to today
    logging.info(f"Fetching data starting from {last_date}...")
    new_data_df = fetch_crypto_data(API_URI)
    logging.info(f"new_data_df head: {new_data_df.head()}")
    logging.info(f"new_data_df tail: {new_data_df.tail()}")
    # Filter the DataFrame to include only new rows
    if latest_entry:
        new_data_df = new_data_df[new_data_df['DATE'] > last_date]

    # If new data is available, insert it into MongoDB
    if not new_data_df.empty:
        data_to_insert = new_data_df.to_dict(orient='records')
        result = collection.insert_many(data_to_insert)
        logging.info(f"Inserted {len(result.inserted_ids)} new records into MongoDB.")
    else:
        logging.info("No new data to insert.")
except Exception as e:
    raise e
