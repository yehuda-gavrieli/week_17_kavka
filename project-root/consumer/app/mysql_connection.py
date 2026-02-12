import mysql.connector

db_sql = mysql.connector.connect(
    host="mysql",
    user="root",
    password="password",
    database="suspicious_sql"
)