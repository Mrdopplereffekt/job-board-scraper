import sys
import scrapy
import os
import logging
import psycopg2
import time
import multiprocessing
from scrapy.crawler import CrawlerProcess
from job_board_scraper.spiders.greenhouse_jobs_outline_spider import (
    GreenhouseJobsOutlineSpider,
)
from job_board_scraper.spiders.greenhouse_job_departments_spider import (
    GreenhouseJobDepartmentsSpider,
)
from job_board_scraper.utils.postgres_wrapper import PostgresWrapper
from job_board_scraper.utils import general as util
from job_board_scraper.utils.scraper_util import get_url_chunks
from scrapy.utils.project import get_project_settings

logger = logging.getLogger("logger")
run_hash = util.hash_ids.encode(int(time.time()))

# Conditionally import LeverJobsOutlineSpider
try:
    from job_board_scraper.spiders.lever_jobs_outline_spider import LeverJobsOutlineSpider
    LEVER_AVAILABLE = True
except ImportError:
    logger.warning("LeverJobsOutlineSpider not available - skipping Lever job boards")
    LEVER_AVAILABLE = False


def run_spider(single_url_chunk, chunk_number):
    process = CrawlerProcess(get_project_settings())
    for i, careers_page_url in enumerate(single_url_chunk):
        logger.info(f"url = {careers_page_url}")
        if careers_page_url.split(".")[1] == "greenhouse":
            process.crawl(
                GreenhouseJobDepartmentsSpider,
                careers_page_url=careers_page_url,
                run_hash=run_hash,
                url_id=chunk_number * len(single_url_chunk) + i,
            )
            process.crawl(
                GreenhouseJobsOutlineSpider,
                careers_page_url=careers_page_url,
                run_hash=run_hash,
                url_id=chunk_number * len(single_url_chunk) + i,
            )
        elif careers_page_url.split(".")[1] == "lever" and LEVER_AVAILABLE:
            process.crawl(
                LeverJobsOutlineSpider,
                careers_page_url=careers_page_url,
                run_hash=run_hash,
                url_id=chunk_number * len(single_url_chunk) + i,
            )
    process.start()


# Function to initialize the database with company URLs
def initialize_database():
    # Connect to the database
    connection = psycopg2.connect(
        host=os.environ.get("PG_HOST"),
        user=os.environ.get("PG_USER"),
        password=os.environ.get("PG_PASSWORD"),
        dbname=os.environ.get("PG_DATABASE"),
    )
    
    cursor = connection.cursor()
    
    # Create the company URLs table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS company_urls (
        id SERIAL PRIMARY KEY,
        url VARCHAR(255) NOT NULL,
        is_enabled BOOLEAN DEFAULT TRUE,
        company_name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # Create job posting tables if they don't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS greenhouse_job_departments (
        id VARCHAR(255) PRIMARY KEY,
        department_id VARCHAR(255),
        department_name VARCHAR(255),
        department_category VARCHAR(255),
        created_at BIGINT,
        updated_at BIGINT,
        source VARCHAR(255),
        company_name VARCHAR(255),
        run_hash VARCHAR(255)
    );
    
    CREATE TABLE IF NOT EXISTS greenhouse_jobs_outline (
        id VARCHAR(255) PRIMARY KEY,
        department_ids VARCHAR(255),
        office_ids VARCHAR(255),
        opening_title VARCHAR(255),
        opening_link VARCHAR(1024),
        location VARCHAR(255),
        created_at BIGINT,
        updated_at BIGINT,
        source VARCHAR(255),
        run_hash VARCHAR(255)
    );
    """)
    
    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    # Initialize the database tables
    initialize_database()
    
    chunk_size = int(os.environ.get("CHUNK_SIZE", 1))
    try:
        postgres_wrapper = PostgresWrapper()
        urls_to_scrape = postgres_wrapper.query(os.environ.get("PAGES_TO_SCRAPE_QUERY", "select distinct url from company_urls where is_enabled=true;"))
        chunks = get_url_chunks(urls_to_scrape, chunk_size)
        multiprocessing.Pool(len(chunks)).starmap(run_spider, enumerate(chunks))
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e
