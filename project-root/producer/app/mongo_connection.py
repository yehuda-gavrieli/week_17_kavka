from pymongo import MongoClient


mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["suspicious_db"]
collection = db["customers_orders"]