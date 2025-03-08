import sys
import scrapy
import os
import logging
import psycopg2
import time
import multiprocessing
import requests
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
from urllib.parse import urlparse

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


class DatabaseManager:
    """A simple database connection manager to reuse connections"""
    
    def __init__(self):
        self.connection = None
    
    def get_connection(self):
        """Get an existing connection or create a new one"""
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(
                host=os.environ.get("PG_HOST"),
                user=os.environ.get("PG_USER"),
                password=os.environ.get("PG_PASSWORD"),
                dbname=os.environ.get("PG_DATABASE"),
            )
        return self.connection
    
    def execute_query(self, query, params=None, fetch=True):
        """Execute a query and optionally return results"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, params)
            
            if fetch:
                results = cursor.fetchall()
            else:
                results = None
                
            conn.commit()
            return results
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def close(self):
        """Close the connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.connection = None


def verify_urls(urls_to_verify, db_connection=None):
    """
    Verify URLs before crawling them.
    Returns a tuple of (valid_urls, invalid_urls)
    """
    import time
    
    valid_urls = []
    invalid_urls = []
    
    logger.info(f"Verifying {len(urls_to_verify)} URLs...")
    
    # Create a connection if not provided
    connection_created = False
    if db_connection is None:
        db_connection = psycopg2.connect(
            host=os.environ.get("PG_HOST"),
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            dbname=os.environ.get("PG_DATABASE"),
        )
        connection_created = True
    
    for url_tuple in urls_to_verify:
        url = url_tuple[0]  # Extract the URL from the tuple
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # First try a HEAD request (faster)
            try:
                head_response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
                response = head_response
                request_type = "HEAD"
            except requests.exceptions.RequestException:
                # If HEAD fails, try GET as fallback (more compatible)
                logger.info(f"HEAD request failed for {url}, trying GET...")
                get_response = requests.get(url, headers=headers, timeout=15, allow_redirects=True, stream=True)
                # Close the connection to avoid downloading the entire content
                get_response.close()
                response = get_response
                request_type = "GET"
            
            # If we get a 2xx or 3xx status code, consider the URL valid
            if 200 <= response.status_code < 400:
                valid_urls.append(url_tuple)
                logger.info(f"URL verified ({request_type}): {url} (Status: {response.status_code})")
            else:
                invalid_urls.append((url, response.status_code))
                logger.warning(f"Invalid URL ({request_type}): {url} (Status: {response.status_code})")
                
                # Mark as disabled in the database
                cursor = db_connection.cursor()
                cursor.execute(
                    "UPDATE company_urls SET is_enabled=false, updated_at=CURRENT_TIMESTAMP WHERE url=%s;",
                    (url,)
                )
                db_connection.commit()
                cursor.close()
                
        except requests.exceptions.RequestException as e:
            invalid_urls.append((url, str(e)))
            logger.warning(f"Error verifying URL: {url} - {str(e)}")
            
            # Mark as disabled in the database
            try:
                cursor = db_connection.cursor()
                cursor.execute(
                    "UPDATE company_urls SET is_enabled=false, updated_at=CURRENT_TIMESTAMP WHERE url=%s;",
                    (url,)
                )
                db_connection.commit()
                cursor.close()
            except Exception as db_error:
                logger.error(f"Database error: {str(db_error)}")
        
        # Add a small delay to prevent overloading servers
        time.sleep(0.5)
    
    # Close the connection if we created it
    if connection_created:
        db_connection.close()
    
    return valid_urls, invalid_urls


def run_spider(single_url_chunk, chunk_number):
    """Run spiders for a chunk of URLs"""
    logger.info(f"Processing chunk {chunk_number} with {len(single_url_chunk)} URLs")
    process = CrawlerProcess(get_project_settings())
    for i, careers_page_url in enumerate(single_url_chunk):
        logger.info(f"url = {careers_page_url}")
        
        # Use proper URL parsing for more reliable board type detection
        parsed_url = urlparse(careers_page_url)
        
        # Check for Greenhouse job boards
        if "greenhouse.io" in parsed_url.netloc:
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
        # Check for Lever job boards
        elif "lever.co" in parsed_url.netloc and LEVER_AVAILABLE:
            process.crawl(
                LeverJobsOutlineSpider,
                careers_page_url=careers_page_url,
                run_hash=run_hash,
                url_id=chunk_number * len(single_url_chunk) + i,
            )
    process.start()


# Function to initialize the database with company URLs
def initialize_database():
    # Create a database manager
    db_manager = DatabaseManager()
    
    # Create the company URLs table if it doesn't exist
    # Adding a UNIQUE constraint to the url column
    db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS company_urls (
        id SERIAL PRIMARY KEY,
        url VARCHAR(255) NOT NULL,
        is_enabled BOOLEAN DEFAULT TRUE,
        company_name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """, fetch=False)
    
    # Check if the table exists and has the UNIQUE constraint
    try:
        # Try to add UNIQUE constraint to the url column if it doesn't already exist
        db_manager.execute_query("""
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
        """, fetch=False)
        logger.info("Ensured UNIQUE constraint on url column")
    except Exception as e:
        logger.error(f"Error ensuring UNIQUE constraint: {str(e)}")
    
    # Create job posting tables if they don't exist
    db_manager.execute_query("""
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
    """, fetch=False)
    
    # Check if we have any company URLs
    url_count_result = db_manager.execute_query("SELECT COUNT(*) FROM company_urls;")
    url_count = url_count_result[0][0] if url_count_result else 0
    
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
                # Verify URL before adding
                valid = verify_url_before_adding(url)
                
                # Simpler insert without ON CONFLICT to avoid issues
                db_manager.execute_query(
                    "INSERT INTO company_urls (url, company_name, is_enabled) VALUES (%s, %s, %s);",
                    (url, company_name, valid),
                    fetch=False
                )
                logger.info(f"Added URL {url} with is_enabled={valid}")
            except psycopg2.errors.UniqueViolation:
                logger.info(f"URL {url} already exists, skipping")
            except Exception as e:
                logger.error(f"Error inserting URL {url}: {str(e)}")
        
        logger.info(f"Added default company URLs.")
    
    # Close the database connection
    db_manager.close()


def verify_url_before_adding(url):
    """Verify a single URL before adding it to the database"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Try HEAD first
        try:
            head_response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            response = head_response
        except requests.exceptions.RequestException:
            # Fallback to GET
            get_response = requests.get(url, headers=headers, timeout=15, allow_redirects=True, stream=True)
            get_response.close()  # Close to avoid downloading everything
            response = get_response
        
        return 200 <= response.status_code < 400
        
    except requests.exceptions.RequestException:
        return False


def execute_query(query):
    """Execute a query and return the results"""
    # Create a database manager
    db_manager = DatabaseManager()
    
    try:
        # Execute the query
        results = db_manager.execute_query(query)
        return results
    finally:
        # Close the connection
        db_manager.close()


def run_single_spider(url):
    """Run a single spider without multiprocessing"""
    logger.info(f"Running single spider for URL: {url}")
    process = CrawlerProcess(get_project_settings())
    
    # Use proper URL parsing for more reliable board type detection
    parsed_url = urlparse(url)
    
    # Check for Greenhouse job boards
    if "greenhouse.io" in parsed_url.netloc:
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
    # Check for Lever job boards
    elif "lever.co" in parsed_url.netloc and LEVER_AVAILABLE:
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
    
    # Create a database manager for this session
    db_manager = DatabaseManager()
    
    chunk_size = int(os.environ.get("CHUNK_SIZE", 1))
    try:
        # Get the query to retrieve URLs to scrape
        query_string = os.environ.get(
            "PAGES_TO_SCRAPE_QUERY", 
            "select distinct url from company_urls where is_enabled=true;"
        )
        
        # Execute the query using the connection manager
        urls_to_scrape = db_manager.execute_query(query_string)
        logger.info(f"Found {len(urls_to_scrape)} URLs to scrape")
        
        if not urls_to_scrape:
            logger.warning("No URLs found to scrape. Please add URLs to the company_urls table.")
            db_manager.close()
            sys.exit(0)
        
        # Verify URLs before processing, reusing the connection
        valid_urls, invalid_urls = verify_urls(urls_to_scrape, db_manager.get_connection())
        
        logger.info(f"Valid URLs: {len(valid_urls)}, Invalid URLs: {len(invalid_urls)}")
        
        if not valid_urls:
            logger.warning("No valid URLs found to scrape after verification.")
            db_manager.close()
            sys.exit(0)
        
        chunks = get_url_chunks(valid_urls, chunk_size)
        logger.info(f"Split URLs into {len(chunks)} chunks of size {chunk_size}")
        
        # Close the connection before starting multiprocessing
        db_manager.close()
        
        if len(chunks) > 0:
            # Use multiprocessing if we have multiple chunks
            if len(chunks) == 1:
                # If we only have one chunk, just run it directly without multiprocessing
                run_spider(chunks[0], 0)
            else:
                # Create a list of arguments for starmap
                args = [(chunk, i) for i, chunk in enumerate(chunks)]
                
                # Use multiprocessing to process chunks in parallel
                with multiprocessing.Pool(processes=min(len(chunks), multiprocessing.cpu_count())) as pool:
                    pool.starmap(run_spider, args)
        else:
            logger.warning("No URL chunks to process.")
            
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        # If we have a database manager, close it
        if 'db_manager' in locals():
            db_manager.close()
