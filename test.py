from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv
import os 
load_dotenv()
mongo_uri = os.getenv("PYMONGO_URI")    
print(mongo_uri) 
client = MongoClient(mongo_uri) 
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)