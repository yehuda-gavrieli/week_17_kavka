import json
from mysql_connection import db_sql
from confluent_kafka import Consumer


cursor = db_sql.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS customers 
                   (customerNumber INT PRIMARY KEY,
                    customerName VARCHAR(255),
                    contactLastName VARCHAR(255),
                    contactFirstName VARCHAR(255),
                    phone VARCHAR(255),
                    addressLine1 VARCHAR(255),
                    addressLine2 VARCHAR(255),
                    city VARCHAR(255),
                    state VARCHAR(255),
                    postalCode VARCHAR(255),
                    country VARCHAR(255),
                    salesRepEmployeeNumber INT
                    creditLimit DECIMAL(10,2))""")


cursor.execute("""CREATE TABLE IF NOT EXISTS orders
                   (orderNumber INT PRIMARY KEY,
                    orderDate VARCHAR(255),
                    requiredDate VARCHAR(255)
                    shippedDate VARCHAR(255)
                    status VARCHAR(255),
                    comments VARCHAR(255)
                    customerNumber INT FOREIGN KEY,)""")


consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['topic_transaction'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("❌ Error:", msg.error())
        continue
    
    data = json.loads(msg.value().decode('utf-8'))
    
    if data['type'] == 'customer':
        sql = """INSERT IGNORE INTO customers 
        (customerNumber,
          customerName,
          contactLastName,
          contactFirstName,
          phone,
          addressLine1,
          addressLine2,
          city,
          state,
          postalCode,
          country,
          salesRepEmployeeNumber,
          creditLimit)
        VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)"""
        cursor.execute(sql, (data['customerNumber'], data['customerName'], data['contactLastName'], data['contactFirstName'], data['phone'], data['addressLine1'], data['addressLine2'], data['city'], data['state'],data['postalCode'],data['country'],data['salesRepEmployeeNumber'], data['creditLimit']))
    
    elif data['type'] == 'order':
        sql = """INSERT IGNORE INTO orders 
        (orderNumber,
        orderDate,
        requiredDate,
        shippedDate,
        status,
        comments,
        customerNumber,) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql, (data['orderNumber'],data['orderDate'],data['requiredDate'],data['shippedDate'], data['status'], data['comments'],data['customerNumber']))
    
    db_sql.commit()



