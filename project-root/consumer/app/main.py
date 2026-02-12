import json
import mysql.connector
from confluent_kafka import Consumer


db_sql = mysql.connector.connect(
    host="mysql",
    user="root",
    password="password",
    database="suspicious_sql"
)
cursor = db_sql.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customerNumber INT PRIMARY KEY,
    customerName VARCHAR(255),
    creditLimit DECIMAL(10,2)
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    orderNumber INT PRIMARY KEY,
    customerNumber INT,
    status VARCHAR(50),
    FOREIGN KEY (customerNumber) REFERENCES customers(customerNumber)
)
""")

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['transactions_topic'])

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    
    data = json.loads(msg.value().decode('utf-8'))
    
    if data['type'] == 'customer':
        sql = "INSERT IGNORE INTO customers (customerNumber, customerName, creditLimit) VALUES (%s, %s, %s)"
        cursor.execute(sql, (data['customerNumber'], data['customerName'], data['creditLimit']))
    
    elif data['type'] == 'order':
        sql = "INSERT IGNORE INTO orders (orderNumber, customerNumber, status) VALUES (%s, %s, %s)"
        cursor.execute(sql, (data['orderNumber'], data['customerNumber'], data['status']))
    
    db_sql.commit()