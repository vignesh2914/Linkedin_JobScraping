import os
import sys
import time
import requests
from bs4 import BeautifulSoup as bs
import csv
import mysql.connector as conn
import datetime
from mysql.connector import Error
from database import connect_to_mysql_database, create_cursor_object
from dotenv import load_dotenv

def configure():
    load_dotenv()

configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")

# Function to create the URL based on user input
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
            final_url = default_selected_filter.format(job_keyword, location_keyword)
            return final_url
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to scrape job data from LinkedIn
def scrape_job_data(job_url, max_pages=100, time_limit=30):
    job_data = []
    start_time = time.time()
    page_number = 0

    while page_number < max_pages and time.time() - start_time <= time_limit:
        next_page = f"{job_url}&start={page_number * 25}"
        response = requests.get(next_page)
        try:
            response.raise_for_status()
            if response.status_code == 429:
                time.sleep(10)
                continue

            soup = bs(response.content, 'html.parser')
            jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')

            if not jobs:
                break

            for job in jobs:
                job_title = job.find('h3', class_='base-search-card__title').text.strip()
                job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
                job_location = job.find('span', class_='job-search-card__location').text.strip()
                job_link = job.find('a', class_='base-card__full-link')['href']

                job_data.append({
                    'ROLE': job_title,
                    'Company_Name': job_company,
                    'Location': job_location,
                    'Link': job_link
                })

            page_number += 1

            if page_number % 50 == 0:
                time.sleep(30)

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                time.sleep(10)
                continue

    return job_data


def save_data_into_db(job_data):
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)

        for job in job_data:
            link = job['Link'][:255] if len(job['Link']) > 255 else job['Link']
            cursor.execute('INSERT INTO linked.job_data (ROLE, COMPANY_NAME, LOCATION, LINK) VALUES (%s, %s, %s, %s)', (job['ROLE'], job['COMPANY_NAME'], job['LOCATION'], link))
            print("Job details saved in DB successfully")
        mydb.commit()
        mydb.close()
    except:
        pass
    
final_url = make_url()
job_data = scrape_job_data(final_url)
save_data_into_db(job_data)
