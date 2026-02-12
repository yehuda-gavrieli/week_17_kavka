import json
import time
from pymongo import MongoClient
from confluent_kafka import Producer


mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["intelligence_db"]
collection = db["transactions"]

kafka_config = {'bootstrap.servers': 'kafka:9092'}
producer = Producer(kafka_config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def run_producer():
    docs = list(collection.find())
    
    for doc in docs:
        doc['_id'] = str(doc['_id']) 
        producer.produce('transactions_topic', json.dumps(doc).encode('utf-8'), callback=delivery_report)
        producer.flush()
        print(f"Sent: {doc.get('customerNumber')}")
        time.sleep(0.5) 

if __name__ == "__main__":
    run_producer()