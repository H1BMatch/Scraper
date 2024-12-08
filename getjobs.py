import psycopg2
from jobspy import scrape_jobs
from datetime import datetime
from collections import deque
import logging
from psycopg2.extras import execute_values
import schedule
import time
from config import job_titles, locations
from dotenv import load_dotenv
import os

load_dotenv()


db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

import logging
from datetime import datetime

def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
   
    logging.basicConfig(
        format=log_format,
        datefmt=date_format,
        level=logging.INFO,
        handlers=[
            logging.StreamHandler()  # Log to the console
        ]
    )
    

    jobspy_logger = logging.getLogger('JobSpy')
    jobspy_logger.setLevel(logging.INFO)
    jobspy_handler = logging.StreamHandler()
    jobspy_handler.setFormatter(logging.Formatter(log_format, date_format))
    jobspy_logger.addHandler(jobspy_handler)
    
    logging.info("Logging started for job scraping session.")
setup_logging()

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432"
        )
        print("connected")
        logging.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(e)
        logging.error(f"Error connecting to database: {e}")
        return None

def get_existing_job_ids(conn, job_ids):
    if not job_ids:
        return set()
    
    try:
        cur = conn.cursor()
        query = "SELECT id FROM jobs WHERE id = ANY(%s)"
        cur.execute(query, (job_ids,))
        existing_job_ids = set(row[0] for row in cur.fetchall())
        cur.close()
        return existing_job_ids
    except Exception as e:
        logging.error(f"Error checking job IDs in the database: {e}")
        return set()

def batch_insert_jobs(conn, new_jobs):
    if not new_jobs:
        logging.info("No new jobs to insert.")
        return 0

    try:
        cur = conn.cursor()
        insert_query = """
            INSERT INTO Jobs (id, title, company, job_url, country, city, state, 
                                 description, job_type, min_amount, max_amount, salary_interval, 
                                 currency, date_posted, emails, is_remote, company_industry, 
                                 company_country, job_level, ceo_name, logo_photo_url, 
                                 banner_photo_url, scraped_at) 
            VALUES %s
        """
    
        insert_values = [
            (
                job.get('id', None),  
                job.get('title', None),
                job.get('company', None),
                job.get('job_url', None),
                job.get('country', None),
                job.get('city', None),
                job.get('state', None),
                job.get('description', None),
                job.get('job_type', None),
                job.get('min_amount', None),
                job.get('max_amount', None),
                job.get('interval', None),
                job.get('currency', None),
                job.get('date_posted', datetime.now()),  
                job.get('emails', None),
                job.get('is_remote', None),
                job.get('company_industry', None),
                job.get('company_country', None),
                job.get('job_level', None),
                job.get('ceo_name', None),
                job.get('logo_photo_url', None),
                job.get('banner_photo_url', None),
                datetime.now()  
            )
            for job in new_jobs
        ]

       
        execute_values(cur, insert_query, insert_values)
        conn.commit()
        cur.close()

        logging.info(f"Inserted {len(new_jobs)} new jobs.")
        return len(new_jobs)
    except Exception as e:
        logging.error(f"Error inserting new jobs: {e}")
        return 0

# Step 5: Scrape jobs and process them
def scrape_and_save_jobs(conn, job_title, location):
    #  hours_old=72,
    try:
        # Step 1: Scrape jobs
        jobs = scrape_jobs(
            site_name=["indeed"],
            search_term=job_title,
            location=location,
            results_wanted=1000,
            country_indeed='USA',
            linkedin_fetch_description=True
        )
        logging.info(f"Found {len(jobs)} jobs for {job_title} in {location}")

        job_ids = []
        for _, job in jobs.iterrows():
            job_ids.append(job['id'])
        logging.info(f"Checking {len(job_ids)} jobs for duplicates in the database.")
        existing_job_ids = get_existing_job_ids(conn, job_ids)
        logging.info(f"Found {len(existing_job_ids)} existing jobs in the database.")
        
        new_jobs = []
        for _, job in jobs.iterrows():
            if job['id'] not in existing_job_ids:
                new_jobs.append(job)
        logging.info(f"Found {len(new_jobs)} new jobs to insert.")

        # Step 5: Insert new jobs in a batch
        total_inserted = batch_insert_jobs(conn, new_jobs)
        return len(jobs), total_inserted

    except Exception as e:
        logging.error(f"Error scraping jobs for {job_title} in {location}: {e}")
        return 0, 0

def rotate_queues(job_titles, locations):
    job_titles_queue = deque(job_titles)
    locations_queue = deque(locations)

    conn = connect_db()
    if conn is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    total_jobs_retrieved = 0
    total_jobs_inserted = 0

    while job_titles_queue:
        current_job_title = job_titles_queue.popleft()  # Get the next job title

        for _ in range(len(locations_queue)):
            current_location = locations_queue.popleft()  # Get the next location
            logging.info(f"Processing: {current_job_title} in {current_location}")

            jobs_retrieved, jobs_inserted = scrape_and_save_jobs(conn, current_job_title, current_location)
            total_jobs_retrieved += jobs_retrieved
            total_jobs_inserted += jobs_inserted

            
            locations_queue.append(current_location)

            logging.info(f"Completed: {current_job_title} in {current_location}")

  
    logging.info(f"Total jobs retrieved: {total_jobs_retrieved}")
    logging.info(f"Total jobs inserted: {total_jobs_inserted}")

    conn.close()


def job_scraping_task():

    print("Starting job scraping process.")
    logging.info("Starting job scraping process.")
    rotate_queues(job_titles, locations)
    logging.info("Job scraping process completed.")

if __name__ == "__main__":
  
    schedule.every(30).minutes.do(job_scraping_task)
    rotate_queues(job_titles, locations)

    while True:
        schedule.run_pending()
        time.sleep(30)