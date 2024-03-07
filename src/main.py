import os
import sys
import time
import requests
import csv
import pandas as pd
from datetime import timedelta
from bs4 import BeautifulSoup as bs
from exception import CustomException
from logger import logging
from datetime import datetime
from database import connect_to_mysql_database, create_cursor_object
from dotenv import load_dotenv
from utils import get_current_utc_datetime, extract_utc_date_and_time


def configure():
    load_dotenv()


configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")


# this fun is responsible for creating url based on user input
def make_url():
    try:
        job_keyword = input("Enter the job keywords: ")
        location_keyword = input("Enter the location: ")
        date_posted = int(input("Enter the value (1-4): "))

        default_url = {
            1: 'https://www.linkedin.com/jobs/search/?keywords={}&location={}&origin=JOB_SEARCH_PAGE_LOCATION_HISTORY&refresh=true',
            2: 'https://www.linkedin.com/jobs/search/?f_TPR=r2592000&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true',
            3: 'https://www.linkedin.com/jobs/search/?f_TPR=r604800&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true',
            4: 'https://www.linkedin.com/jobs/search/?f_TPR=r86400&keywords={}&location={}&origin=JOB_SEARCH_PAGE_JOB_FILTER&refresh=true'
        }

        if 1 <= date_posted <= 4:
            default_selected_filter = default_url[date_posted]
            logging.info(f"user selected default url collected - {default_selected_filter}")
            final_url = default_selected_filter.format(job_keyword, location_keyword)
            logging.info(f"final url collected with job and location - {final_url}")
            return final_url
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)


# this fun is responsible for scrap the data from linkedin
def scrape_job_data(job_url, max_pages=100, time_limit=20):
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


def save_data_into_db(job_data):
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        utc_datetime = get_current_utc_datetime()
        date, time = extract_utc_date_and_time(utc_datetime)


        for job in job_data:
            link = job['LINK'][:255] if len(job['LINK']) > 255 else job['LINK']
            cursor.execute('INSERT INTO linkedin2.job_data (DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s, %s,%s, %s, %s, %s)', (date, time, job['ROLE'], job['COMPANY_NAME'], job['LOCATION'], link))
            logging.info("Job details saved in DB successfully")
        mydb.commit()
        mydb.close()
        logging.info("DB closed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)
    

def recent_scrapped_data():
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        logging.info("Connected to the MySQL database successfully.")
        
        cursor = create_cursor_object(mydb)
        
        recent_data_query = "SELECT * FROM linkedin2.job_data WHERE TIMESTAMP(CONCAT(DATE, ' ', TIME)) >= %s"
        logging.info(f"Executing SQL query: {recent_data_query}")
    
        threshold_datetime = datetime.utcnow() - timedelta(seconds=20)
        threshold_time_str = threshold_datetime.strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(recent_data_query, (threshold_time_str,))
        
        recent_data = cursor.fetchall()
        logging.info(f"Fetched {len(recent_data)} records from the database.")

        cursor.close()
        mydb.close()

        logging.info("DB connection closed.")
        return recent_data

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)


def recent_scrapped_data_to_csv(recent_data):
    try:
        if recent_data:
            column_names = ["ID", "DATE", "TIME", "ROLE", "COMPANY_NAME", "LOCATION", "LINK"]
            df = pd.DataFrame(recent_data, columns=column_names)

            folder_name = "Recent_Data"
            os.makedirs(folder_name, exist_ok=True)

            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")

            df.to_csv(csv_file_path, index=False)
            logging.info(f"Recent updated data has been saved to {csv_file_path}")
        else:
            logging.info("No recent data found to save.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)


def save_filtered_data_to_db():
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        utc_datetime = get_current_utc_datetime()
        date, time = extract_utc_date_and_time(utc_datetime)

        cursor.execute("SELECT DISTINCT COMPANY_NAME FROM linkedin2.job_filtered_data")
        existing_companies = {row[0] for row in cursor.fetchall()}


        cursor.execute("SELECT ROLE, COMPANY_NAME, LOCATION, LINK FROM linkedin2.job_data")
        job_data = cursor.fetchall()


        filtered_job_data = []

        for job in job_data:
            company_name = job[1]  # Extract the company name from the current job
            if company_name not in existing_companies:
        # If the company name is not in the set of existing companies, 
        # it means this job is from a new company
                filtered_job_data.append(job)  # Add the job to the filtered job data
                existing_companies.add(company_name)  # Add the company name to the set of existing companies


#                 job_data = [
#     ("Data Analyst", "Company A", "Location A", "Link A"),
#     ("Software Engineer", "Company B", "Location B", "Link B"),
#     ("Data Scientist", "Company C", "Location C", "Link C"),
#     ("Project Manager", "Company A", "Location D", "Link D"),
# ]


        for job in filtered_job_data:
            link = job[3][:255] if len(job[3]) > 255 else job[3]
            cursor.execute('INSERT INTO linkedin2.job_filtered_data (DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s, %s, %s, %s, %s, %s)',
                           (date, time, job[0], job[1], job[2], link))
            logging.info("Filtered job details saved in DB successfully")

        mydb.commit()
        mydb.close()
        logging.info("DB closed with filterd data successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)



def extract_recent_filtered_data():
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        logging.info("Connected to the MySQL database successfully.")
        
        cursor = create_cursor_object(mydb)
        
        recent_data_query = "SELECT * FROM linkedin2.job_filtered_data WHERE TIMESTAMP(CONCAT(DATE, ' ', TIME)) >= %s"
        logging.info(f"Executing SQL query: {recent_data_query}")
    
        threshold_datetime = datetime.utcnow() - timedelta(seconds=20)
        threshold_time_str = threshold_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute(recent_data_query, (threshold_time_str,))
        
        recent_filtered_data = cursor.fetchall()
        logging.info(f"Fetched {len(recent_filtered_data)} records from the database.")

        cursor.close()
        mydb.close()
        logging.info("DB connection closed.")
        
        return recent_filtered_data

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)

def recent_filtered_data_to_csv(recent_filtered_data):
    try:
        if recent_filtered_data:
            column_names = ["ID", "DATE", "TIME", "ROLE", "COMPANY_NAME", "LOCATION", "LINK"]
            df = pd.DataFrame(recent_filtered_data, columns=column_names)

            folder_name = "Filtered_Recent_Data"
            os.makedirs(folder_name, exist_ok=True)

            current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")

            df.to_csv(csv_file_path, index=False)
            logging.info(f"Recent filtered job data has been saved to {csv_file_path}")
        else:
            logging.info("No recent filtered data found to save.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)

    
final_url = make_url()

job_data = scrape_job_data(final_url)
save_data_into_db(job_data)
recent_data = recent_scrapped_data()

save_filtered_data_to_db()
recent_filtered_data = extract_recent_filtered_data()

# Save both recent data and recent filtered data to CSV files
recent_scrapped_data_to_csv(recent_data)
recent_filtered_data_to_csv(recent_filtered_data)





























# def saved_data_to_csv():
#     mydb = None  # Initialize mydb variable
#     try:
#         mydb = connect_to_mysql_database(host, user, password, database)
#         cursor = create_cursor_object(mydb)
        
#         # Fetch all data from the database
#         query = "SELECT DATE, TIME, ROLE, COMPANY_NAME, LOCATION, LINK FROM linkedin2.job_data"
#         cursor.execute(query)
#         data = cursor.fetchall()
        
#         # Define the folder name
#         folder_name = "Job_Data"
#         os.makedirs(folder_name, exist_ok=True)  # Create folder if it doesn't exist
        
#         # Define CSV file path with date and time
#         current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#         csv_file_path = os.path.join(folder_name, f"{current_datetime}.csv")
        
#         # Write data to CSV file
#         with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
#             writer = csv.writer(file)
#             # Write header
#             writer.writerow(['DATE', 'TIME', 'ROLE', 'COMPANY_NAME', 'LOCATION', 'LINK'])
#             # Write data rows
#             for row in data:
#                 writer.writerow(row)
        
#         logging.info(f"Data saved to CSV file: {csv_file_path}")
        
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise CustomException(e, sys)
#     finally:
#         if mydb is not None:
#             mydb.close()  # Close the database connection if it was opened

