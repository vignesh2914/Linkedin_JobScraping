import os
from scraper import JobScraper

job_scraper = JobScraper()

job_url = job_scraper.generate_job_url(job_keyword="Python_developer", location_keyword="India", filter_option=2)
job_data = job_scraper.scrape_job_data(job_url, max_pages=120, time_limit=120)
job_data_df = job_scraper.create_dataframe_of_job_data(job_data)
unique_job_df = job_scraper.get_unique_companies_df(job_data_df, column_name='COMPANY_NAME')
job_scraper.save_job_data_dataframe_to_mysql(job_data_df)
job_scraper.save_filtered_job_data_dataframe_to_mysql(unique_job_df)
job_data_from_db = job_scraper.extract_job_data_from_DB()
job_scraper.save_job_data_to_csv(job_data_from_db)
unique_job_data_from_db = job_scraper.extract_unique_job_data_from_db()
job_scraper.save_unique_job_data_to_csv(unique_job_data_from_db)
job_scraper.unique_company_name()
job_scraper.fetch_recently_created_csv('Unique_Data')









