from fastapi import FastAPI
import mysql.connector

app = FastAPI()

def get_db_connection():
    return mysql.connector.connect(
        host="mysql", user="root", password="password", database="suspicious_db"
    )
