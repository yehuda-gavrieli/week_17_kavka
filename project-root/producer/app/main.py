import json
import time
from pymongo import MongoClient
from confluent_kafka import Producer


mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["suspicious_db"]
collection = db["customers_orders"]

kafka_config = {"bootstrap.servers": "kafka:9092"}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def run_producer():
    all_docs = list(collection.find())
    
    for docs in all_docs:
        docs['_id'] = str(docs['_id']) 
        producer.produce('transactions_topic', json.dumps(docs).encode('utf-8'), callback=delivery_report)
        producer.flush()
        print(f"Sent: {docs.get('customerNumber')}")
        time.sleep(0.5) 

if __name__ == "__main__":
    run_producer()