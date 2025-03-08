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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
    # Adding a UNIQUE constraint to the url column
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
    
    # Check if the table exists and has the UNIQUE constraint
    try:
        # Try to add UNIQUE constraint to the url column if it doesn't already exist
        cursor.execute("""
        DO $$
        BEGIN
            -- Check if constraint exists
            IF NOT EXISTS (
                SELECT 1 
                FROM pg_constraint 
                WHERE conname = 'company_urls_url_key'
            ) THEN
                -- Add unique constraint if it doesn't exist
                ALTER TABLE company_urls ADD CONSTRAINT company_urls_url_key UNIQUE (url);
            END IF;
        END $$;
        """)
        connection.commit()
        logger.info("Ensured UNIQUE constraint on url column")
    except Exception as e:
        logger.error(f"Error ensuring UNIQUE constraint: {str(e)}")
        connection.rollback()
    
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
    
    # Check if we have any company URLs
    cursor.execute("SELECT COUNT(*) FROM company_urls;")
    url_count = cursor.fetchone()[0]
    
    # If no URLs exist, add some default ones for testing
    if url_count == 0:
        logger.info("No company URLs found. Adding some default Greenhouse URLs for testing.")
        default_urls = [
            ("https://boards.greenhouse.io/netflix", "Netflix"),
            ("https://boards.greenhouse.io/spotify", "Spotify"),
            ("https://boards.greenhouse.io/airbnb", "Airbnb"),
            ("https://boards.greenhouse.io/doordash", "DoorDash"),
            ("https://boards.greenhouse.io/stripe", "Stripe")
        ]
        
        for url, company_name in default_urls:
            try:
                # Simpler insert without ON CONFLICT to avoid issues
                cursor.execute(
                    "INSERT INTO company_urls (url, company_name) VALUES (%s, %s);",
                    (url, company_name)
                )
                connection.commit()
                logger.info(f"Added URL {url}")
            except psycopg2.errors.UniqueViolation:
                connection.rollback()
                logger.info(f"URL {url} already exists, skipping")
            except Exception as e:
                connection.rollback()
                logger.error(f"Error inserting URL {url}: {str(e)}")
        
        logger.info(f"Added default company URLs.")
    
    cursor.close()
    connection.close()


def execute_query(query):
    """Execute a query and return the results"""
    connection = psycopg2.connect(
        host=os.environ.get("PG_HOST"),
        user=os.environ.get("PG_USER"),
        password=os.environ.get("PG_PASSWORD"),
        dbname=os.environ.get("PG_DATABASE"),
    )
    
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    
    return results


def run_single_spider(url):
    """Run a single spider without multiprocessing"""
    logger.info(f"Running single spider for URL: {url}")
    process = CrawlerProcess(get_project_settings())
    if url.split(".")[1] == "greenhouse":
        process.crawl(
            GreenhouseJobDepartmentsSpider,
            careers_page_url=url,
            run_hash=run_hash,
            url_id=0,
        )
        process.crawl(
            GreenhouseJobsOutlineSpider,
            careers_page_url=url,
            run_hash=run_hash,
            url_id=0,
        )
    elif url.split(".")[1] == "lever" and LEVER_AVAILABLE:
        process.crawl(
            LeverJobsOutlineSpider,
            careers_page_url=url,
            run_hash=run_hash,
            url_id=0,
        )
    process.start()


if __name__ == "__main__":
    # Initialize the database tables
    initialize_database()
    
    chunk_size = int(os.environ.get("CHUNK_SIZE", 1))
    try:
        # Get the query to retrieve URLs to scrape
        query_string = os.environ.get(
            "PAGES_TO_SCRAPE_QUERY", 
            "select distinct url from company_urls where is_enabled=true;"
        )
        
        # Execute the query directly
        urls_to_scrape = execute_query(query_string)
        logger.info(f"Found {len(urls_to_scrape)} URLs to scrape")
        
        if not urls_to_scrape:
            logger.warning("No URLs found to scrape. Please add URLs to the company_urls table.")
            sys.exit(0)
        
        chunks = get_url_chunks(urls_to_scrape, chunk_size)
        logger.info(f"Split URLs into {len(chunks)} chunks of size {chunk_size}")
        
        if len(chunks) > 0:
            # Use multiprocessing if we have multiple chunks
            multiprocessing.Pool(max(1, len(chunks))).starmap(run_spider, enumerate(chunks))
        else:
            logger.warning("get_url_chunks returned empty list. Running without multiprocessing.")
            # If we have URLs but chunks is empty, run for the first URL
            if urls_to_scrape:
                run_single_spider(urls_to_scrape[0][0])
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise e
