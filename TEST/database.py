import mysql.connector as conn
import sys
import os
import pandas as pd
from dotenv import load_dotenv
from src.exception import CustomException
from src.logger import logging
 
 
def configure():
    load_dotenv()
 
 
configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")
 
 
def create_database(host, user, password):
    try:
        mydb = conn.connect(host=host, user=user, password=password)
        cursor = mydb.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS linked")
        logging.info("Database created successfully")
    except Exception as e:
        logging.error(f"An error occurred while creating database: {e}")
        raise CustomException(e, sys)
   

def connect_to_mysql_database(host, user, password, database):
    try:
        mydb = conn.connect(host=host, user=user, password=password, database=database)
        print("Connected to MySQL successfully!")
        return mydb
    except Exception as e:
        print(f"An error occurred: {e}")
   

def create_cursor_object(mydb):
    try:
        cursor = mydb.cursor()
        print("Cursor object obtained successfully!")
        return cursor
    except Exception as e:
        print(f"An error occurred: {e}")

 
def create_tables(host, user, password, database):
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS linked.job_data(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                ROLE VARCHAR(255),
                COMPANY_NAME VARCHAR(255),
                LOCATION VARCHAR(255),
                LINK VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS linked.job_filtered_data(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                ROLE VARCHAR(255),
                COMPANY_NAME VARCHAR(255),
                LOCATION VARCHAR(255),
                LINK VARCHAR(255)
            )
            """
        ]
 
        # Execute table creation queries
        for query in table_queries:
            cursor.execute(query)
 
        print("Tables and columns created successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

# create_tables(host, user, password, database)

# def saved_data_db_to_csv():
#     try:
#         mydb = connect_to_mysql_database(host, user, password, database)
#         cursor = create_cursor_object(mydb)
        
#         cursor.execute("SELECT ROLE, COMPANY_NAME, LOCATION, LINK FROM linked.job_data")
#         data = cursor.fetchall()
        
#         # Generate a timestamp for the CSV file name
#         timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#         csv_file_path = f"job_data_{timestamp}.csv"  

#         with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
#             writer = csv.writer(file)
#             writer.writerow(['Role', 'Company Name', 'Location', 'Link'])  
#             writer.writerows(data)  
            
#         print(f"Data saved to {csv_file_path} successfully!")
        
#         mydb.close()
#     except Error as e:
#         print(f"An error occurred: {e}")

# saved_data_db_to_csv()

