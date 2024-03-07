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