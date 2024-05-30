from src.logger import logging
from src.exception import CustomException
from typing import List, Dict
import sys
import time
import os
import requests
import pandas as pd
import glob
import os, sys
from typing import Optional
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
from src.database import connect_to_mysql_database, create_cursor_object
from dotenv import load_dotenv
from src.utils import get_current_utc_datetime, extract_utc_date_and_time

class JobScraper:

    def __init__(self):
        load_dotenv()
        self.base_url = {
            1: 'Your url location ={} role = {} analyse it',
            2: 'Your url location ={} role = {} analyse it',
            3: 'Your url location ={} role = {} analyse it',
            4: 'Your url location ={} role = {} analyse it'
        }
        self.host = os.getenv("database_host_name")
        self.user = os.getenv("database_user_name")
        self.password = os.getenv("database_user_password")
        self.database = os.getenv("database_name")

    def generate_job_url(self, job_keyword: str, location_keyword: str, filter_option: int) -> str:
        try:
            if 1 <= filter_option <= 4:
                user_selected_base_url = self.base_url[filter_option]
                logging.info(f"user selected base url is - {user_selected_base_url}")
                job_url = user_selected_base_url.format(job_keyword, location_keyword)
                logging.info(f"final url collected with job and location - {job_url}")
                return job_url
            else:
                raise ValueError("Invalid Filter_option. Filter_option should be between 1 and 4.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)

    def scrape_job_data(self, job_url: str, max_pages: int, time_limit: int) -> list:
        job_data = []
        start_time = time.time()
        page_number = 0
        
        while page_number < max_pages and time.time() - start_time <= time_limit:
            next_page = f"{job_url}&start={page_number * 25}"
            response = requests.get(next_page)
            try:
                response.raise_for_status()
                if response.status_code == 429:
                    logging.info("Rate limit exceeded. Waiting for 10 seconds before retrying...")
                    time.sleep(10)
                    continue
                soup = bs(response.content, 'html.parser')
                jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
                if not jobs:
                    logging.info("No more jobs found.")
                    break
                for job in jobs:
                    job_title = job.find('h3', class_='base-search-card__title').text.strip()
                    job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
                    job_location = job.find('span', class_='job-search-card__location').text.strip()
                    job_link = job.find('a', class_='base-card__full-link')['href']
                    
                    job_data.append({
                    'ROLE': job_title,
                    'COMPANY_NAME': job_company,
                    'LOCATION': job_location,
                    'LINK': job_link
                    })
                    logging.info(f'Data updated. Total records: {page_number * 25 + len(jobs)}')
                    page_number += 1
                    if page_number % 50 == 0:
                        logging.info("Taking a break after fetching 50 records. Sleeping for 30 seconds.")
                        time.sleep(30)
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    logging.info("Rate limit exceeded. Waiting for 10 seconds before retrying...")
                    time.sleep(10)
                    continue
                else:
                    logging.error(f"An error occurred: {e}")
                    logging.info("Data fetching completed.")
        return job_data
    
    def create_dataframe_of_job_data(self, job_data: List[Dict[str, str]]) -> pd.DataFrame:
        try:
            if job_data:
                column_names = ["ROLE", "COMPANY_NAME", "LOCATION", "LINK"]
                df = pd.DataFrame(job_data, columns=column_names)
                logging.info("Data converted into dataframe")
                return df
            else:
                logging.info("No job data found to create dataframe.")
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
    
    def get_unique_companies_df(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        try:
            #data_frame = create_dataframe_of_job_data(job_data)
            filtered_df = df.drop_duplicates(subset=[column_name])
            logging.info("unique company name dataframe created")
            return filtered_df
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
   
    def save_job_data_dataframe_to_mysql(self, df: pd.DataFrame) -> None:
        try:
            mydb = connect_to_mysql_database(self.host, self.user, self.password, self.database)
            cursor = create_cursor_object(mydb)
            utc_datetime = get_current_utc_datetime()
            date, current_time = extract_utc_date_and_time(utc_datetime)
            # Insert DataFrame records into the MySQL table
            
            for index, row in df.iterrows():
                sql = "INSERT INTO linkedin2.job_data (DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s,%s,%s, %s, %s, %s)"
                link = row['LINK'][:255] if len(row['LINK']) > 255 else row['LINK']
                values = (date, current_time, row['ROLE'], row['COMPANY_NAME'], row['LOCATION'], link)
                cursor.execute(sql, values)
                logging.info("Job details saved in DB successfully")
            mydb.commit()
            mydb.close()
            logging.info("DB closed successfully")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
    
    def save_filtered_job_data_dataframe_to_mysql(self, df: pd.DataFrame) -> None:
        try:
            mydb = connect_to_mysql_database(self.host, self.user, self.password, self.database)
            cursor = create_cursor_object(mydb)
            utc_datetime = get_current_utc_datetime()
            date, current_time = extract_utc_date_and_time(utc_datetime)

            for index, row in df.iterrows():
                sql = "INSERT INTO linkedin2.job_filtered_data (DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s,%s,%s, %s, %s, %s)"
                link = row['LINK'][:255] if len(row['LINK']) > 255 else row['LINK']
                values = (date, current_time, row['ROLE'], row['COMPANY_NAME'], row['LOCATION'], link)
                cursor.execute(sql, values)
                logging.info("Job details saved in DB successfully")
            
            mydb.commit()  
            mydb.close()
            logging.info("DB closed successfully")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
        
    def extract_job_data_from_DB(self):
        try:
            mydb = connect_to_mysql_database(self.host, self.user, self.password, self.database)
            cursor = create_cursor_object(mydb)
            # Fetch recent updated data from the database
            query = "SELECT * FROM linkedin2.job_data WHERE TIMESTAMP(CONCAT(DATE, ' ', TIME)) >= %s"
            
            # Set the threshold timestamp for recent data (adjust as needed)
            threshold_datetime = datetime.utcnow() - timedelta(seconds=60)
            threshold_time_str = threshold_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
            cursor.execute(query, (threshold_time_str,))
            fetched_data = cursor.fetchall()
            logging.info("data fetched successfully")
            # Close the database connection
            
            cursor.close()
            mydb.close()
            logging.info("DB closed")
            
            return fetched_data
        
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
        
    def save_job_data_to_csv(self, job_data: List) -> None: 
        try:
            if job_data:
                column_names = ["ID", "DATE", "TIME", "ROLE", "COMPANY_NAME", "LOCATION", "LINK"]
                df = pd.DataFrame(job_data, columns=column_names)
                folder_name = "Job_Data"
                os.makedirs(folder_name, exist_ok=True)
                current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")
                
                df.to_csv(csv_file_path, index=False)
                logging.info("fetched job data saved in in CSV file successfully")
                
            else:
                logging.info("No recent data found to save.")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
        
    def extract_unique_job_data_from_db(self) -> List:
        try:
            mydb = connect_to_mysql_database(self.host, self.user, self.password, self.database)
            logging.info("Connected to the MySQL database successfully.")
            cursor = create_cursor_object(mydb)
            query = "SELECT * FROM linkedin2.job_filtered_data WHERE TIMESTAMP(CONCAT(DATE, ' ', TIME)) >= %s"
            
            threshold_datetime = datetime.utcnow() - timedelta(seconds=20)
            threshold_time_str = threshold_datetime.strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(query, (threshold_time_str,))
            fetched_filtered_data = cursor.fetchall()
            
            cursor.close()
            mydb.close()
            logging.info("DB connection closed.")
            return fetched_filtered_data
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
    
    def save_unique_job_data_to_csv(self, unique_data: List) -> None:
        try:
            if unique_data:
                column_names = ["ID", "DATE", "TIME", "ROLE", "COMPANY_NAME", "LOCATION", "LINK"]
                df = pd.DataFrame(unique_data, columns=column_names)
                
                folder_name = "Unique_Data"
                os.makedirs(folder_name, exist_ok=True)
                
                current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")
                
                df.to_csv(csv_file_path, index=False)
                logging.info("unique job data saved in csv file successfully")
            
            else:
                logging.info("No recent filtered data found to save.")
                
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
        
    def unique_company_name(self):
        try:
        # Connect to MySQL database
            mydb = connect_to_mysql_database(self.host, self.user, self.password, self.database)
            cursor = create_cursor_object(mydb)
            
            query = "SELECT MIN(ROLE), COMPANY_NAME, MIN(LOCATION), MIN(LINK), MIN(DATE), MIN(TIME) FROM linkedin2.job_data GROUP BY COMPANY_NAME"
            cursor.execute(query)
            company_details = cursor.fetchall()
            
            insert_query = "INSERT INTO linkedin2.unique_data (DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s, %s, %s, %s, %s, %s)"
            for company_detail in company_details:
                values = (company_detail[4], company_detail[5], company_detail[0], company_detail[1], company_detail[2], company_detail[3])
                cursor.execute(insert_query, values)
                
            mydb.commit()
            cursor.close()
            mydb.close()
            logging.info("Unique company details added to unique_data table successfully")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
        
    def fetch_recently_created_csv(job_data_path: str) -> Optional[str]:
        try:
            # Get a list of all CSV files in the directory
            csv_files = glob.glob(os.path.join(job_data_path, '*.csv'))
            if csv_files:
                
                latest_csv_file = max(csv_files, key=os.path.getmtime)
                logging.info(f'latest created file path obtained - {latest_csv_file}')
                return latest_csv_file
            
            else:
                logging.info("No csv file found")
                return None
            
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)

        
    def fetch_recently_created_csv(self, job_data_path: str) -> Optional[str]:
        try:
            csv_files = glob.glob(os.path.join(job_data_path, '*.csv'))
            if csv_files:
                latest_csv_file = max(csv_files, key=os.path.getmtime)
                logging.info(f'latest created file path obtained - {latest_csv_file}')
                return latest_csv_file
            else:
                logging.info("No csv file found")
                return None
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise CustomException(e, sys)
       