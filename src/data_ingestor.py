import os
import logging
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from src.logging.logger import logging
from src.exception_handling.custom_exception import BitCoinException

load_dotenv() 
MONGO_URI = os.getenv("MONGO_URI")

def fetch_data_from_mongodb(collection_name:str, database_name:str):  
    client = MongoClient(MONGO_URI)
    db = client[database_name]  
    collection = db[collection_name]    
    try:
        logging.info(f"Fetching data from MongoDB collection: {collection_name}...")
        data = list(collection.find())  

        if not data:
            logging.info("No data found in the MongoDB collection.")
             
        df = pd.DataFrame(data) 
        if '_id' in df.columns:
            df = df.drop(columns=['_id']) 
        logging.info("Data successfully fetched and converted to a DataFrame!")
        return df 
    except Exception as e:
        logging.error(f"An error occurred while fetching data: {e}")
        raise e  
