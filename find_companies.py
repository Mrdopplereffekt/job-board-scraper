#!/usr/bin/env python3
"""
Company URL Finder for Job Board Scraper

This script discovers companies using various job boards by:
1. Scanning the Greenhouse and Lever job board directories
2. Searching for job board patterns across the web
3. Using an optional list of known companies to check if they use Greenhouse or Lever
4. Storing results in the database for the job scraper to use

Usage:
    python find_companies.py [--all] [--greenhouse] [--lever] [--search] [--known-companies]
"""

import requests
import psycopg2
import os
import time
import random
import logging
import argparse
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# User agents to rotate to avoid being blocked
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"
]

# List of top tech companies to check if they have Greenhouse or Lever job boards
TOP_TECH_COMPANIES = [
    "Netflix", "Spotify", "Airbnb", "DoorDash", "Stripe", "Uber", "Lyft", "Slack",
    "Pinterest", "Shopify", "Dropbox", "Twitter", "Square", "Zoom", "Coinbase",
    "Twilio", "Gitlab", "Notion", "Atlassian", "Figma", "Canva", "Reddit", "Medium",
    "Asana", "MongoDB", "Datadog", "Cloudflare", "Fastly", "Snowflake", "Databricks"
]

class CompanyURLFinder:
    def __init__(self):
        # Database connection details
        self.pg_host = os.environ.get("PG_HOST")
        self.pg_user = os.environ.get("PG_USER")
        self.pg_password = os.environ.get("PG_PASSWORD")
        self.pg_database = os.environ.get("PG_DATABASE")
        
        # Setup connection
        self.conn = psycopg2.connect(
            host=self.pg_host,
            user=self.pg_user,
            password=self.pg_password,
            dbname=self.pg_database
        )
        
        # Initialize database tables if needed
        self.init_database()
        
    def init_database(self):
        """Initialize the database schema if it doesn't exist"""
        with self.conn.cursor() as cursor:
            # Create company_urls table if it doesn't exist
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
            
            # Add UNIQUE constraint if it doesn't exist
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
            
            self.conn.commit()
            logger.info("Database initialized")

    def get_random_user_agent(self):
        """Return a random user agent from the list"""
        return random.choice(USER_AGENTS)
    
    def scan_greenhouse_directory(self):
        """Scan Greenhouse boards page for company listings"""
        
        logger.info("Scanning Greenhouse directory...")
        companies_found = 0
        
        # Simulate browsing through alphabet pages
        for letter in "abcdefghijklmnopqrstuvwxyz":
            url = f"https://boards.greenhouse.io/companies?starts_with={letter}"
            
            try:
                headers = {'User-Agent': self.get_random_user_agent()}
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    company_links = soup.select('.company-list a')
                    
                    for link in company_links:
                        company_name = link.text.strip()
                        greenhouse_url = f"https://boards.greenhouse.io/{link['href'].split('/')[-1]}"
                        
                        # Insert into database
                        self.add_company(company_name, greenhouse_url, "greenhouse")
                        companies_found += 1
                        
                    logger.info(f"Processed Greenhouse companies starting with '{letter}': Found {len(company_links)} companies")
                    
                    # Be nice to Greenhouse servers
                    time.sleep(random.uniform(3, 5))
                else:
                    logger.warning(f"Failed to get Greenhouse companies for letter '{letter}'. Status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error scanning Greenhouse directory for letter '{letter}': {str(e)}")
        
        logger.info(f"Completed Greenhouse directory scan. Found {companies_found} companies.")
        return companies_found
    
    def scan_lever_companies(self):
        """Attempt to discover Lever job boards by checking top companies and from search"""
        
        logger.info("Discovering Lever job boards...")
        companies_found = 0
        
        # Check top tech companies for Lever job boards
        for company in TOP_TECH_COMPANIES:
            company_slug = company.lower().replace(" ", "")
            lever_url = f"https://jobs.lever.co/{company_slug}"
            
            try:
                headers = {'User-Agent': self.get_random_user_agent()}
                response = requests.get(lever_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    # Insert into database
                    self.add_company(company, lever_url, "lever")
                    companies_found += 1
                    logger.info(f"Found Lever job board for {company}: {lever_url}")
                
                # Be nice to Lever servers
                time.sleep(random.uniform(1, 3))
            
            except Exception as e:
                logger.error(f"Error checking Lever job board for {company}: {str(e)}")
        
        logger.info(f"Completed Lever job boards discovery. Found {companies_found} companies.")
        return companies_found
    
    def add_company(self, company_name, job_board_url, job_board_type):
        """Add a company to the database"""
        try:
            # Insert into company_urls table for the scraper
            with self.conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO company_urls (url, company_name)
                VALUES (%s, %s)
                ON CONFLICT (url) DO NOTHING
                """, (job_board_url, company_name))
                
                self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding company {company_name}: {str(e)}")
    
    def search_companies(self, query, board_type, max_results=50):
        """Search for companies using job boards"""
        
        logger.info(f"Searching for companies with query: '{query}'")
        search_url = f"https://www.google.com/search?q={query}&num={max_results}"
        
        companies_found = 0
        
        try:
            headers = {'User-Agent': self.get_random_user_agent()}
            response = requests.get(search_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                search_results = soup.select('.g')
                
                for result in search_results:
                    link_element = result.select_one('a')
                    if link_element and 'href' in link_element.attrs:
                        link = link_element['href']
                        
                        # Extract job board URL if it exists
                        if board_type == 'greenhouse' and 'boards.greenhouse.io' in link:
                            # Extract company name from title
                            title_element = result.select_one('h3')
                            company_name = title_element.text if title_element else "Unknown"
                            
                            # Clean up URL to get proper format
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                job_board_url = f"https://boards.greenhouse.io/{company_slug}"
                                
                                # Add to database
                                self.add_company(company_name, job_board_url, "greenhouse")
                                companies_found += 1
                        
                        elif board_type == 'lever' and 'jobs.lever.co' in link:
                            # Extract company name from title
                            title_element = result.select_one('h3')
                            company_name = title_element.text if title_element else "Unknown"
                            
                            # Clean up URL to get proper format
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                job_board_url = f"https://jobs.lever.co/{company_slug}"
                                
                                # Add to database
                                self.add_company(company_name, job_board_url, "lever")
                                companies_found += 1
            else:
                logger.warning(f"Search request failed with status code: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error searching for companies: {str(e)}")
        
        logger.info(f"Found {companies_found} companies via search for {board_type}")
        return companies_found
    
    def get_stats(self):
        """Get statistics about the companies database"""
        with self.conn.cursor() as cursor:
            # Count total companies
            cursor.execute("SELECT COUNT(*) FROM company_urls")
            total_companies = cursor.fetchone()[0]
            
            # Count enabled URLs for scraping
            cursor.execute("SELECT COUNT(*) FROM company_urls WHERE is_enabled = TRUE")
            enabled_urls = cursor.fetchone()[0]
            
            # Count greenhouse URLs
            cursor.execute("SELECT COUNT(*) FROM company_urls WHERE url LIKE '%boards.greenhouse.io%'")
            greenhouse_urls = cursor.fetchone()[0]
            
            # Count lever URLs
            cursor.execute("SELECT COUNT(*) FROM company_urls WHERE url LIKE '%jobs.lever.co%'")
            lever_urls = cursor.fetchone()[0]
            
            return {
                "total_companies": total_companies,
                "enabled_urls": enabled_urls,
                "greenhouse_urls": greenhouse_urls,
                "lever_urls": lever_urls
            }
    
    def run_full_discovery(self):
        """Run the complete company discovery process"""
        logger.info("Starting Company URL Finder")
        
        # Get initial stats
        initial_stats = self.get_stats()
        
        # Scan Greenhouse directory
        self.scan_greenhouse_directory()
        
        # Discover Lever job boards
        self.scan_lever_companies()
        
        # Search for Greenhouse companies using different search terms
        greenhouse_search_terms = [
            "site:boards.greenhouse.io careers",
            "site:boards.greenhouse.io jobs",
            "apply through greenhouse io",
            "powered by greenhouse",
            "apply for this job greenhouse"
        ]
        
        for term in greenhouse_search_terms:
            self.search_companies(term, "greenhouse")
            time.sleep(random.uniform(10, 15))  # Avoid rate limiting
        
        # Search for Lever companies
        lever_search_terms = [
            "site:jobs.lever.co careers",
            "site:jobs.lever.co apply",
            "apply through lever.co",
            "powered by lever",
            "apply for this job lever"
        ]
        
        for term in lever_search_terms:
            self.search_companies(term, "lever")
            time.sleep(random.uniform(10, 15))  # Avoid rate limiting
        
        # Get final stats
        final_stats = self.get_stats()
        
        logger.info(f"Company URL Finder complete")
        logger.info(f"Initial companies: {initial_stats['total_companies']}")
        logger.info(f"Final companies: {final_stats['total_companies']}")
        logger.info(f"New companies found: {final_stats['total_companies'] - initial_stats['total_companies']}")
        logger.info(f"Greenhouse URLs: {final_stats['greenhouse_urls']}")
        logger.info(f"Lever URLs: {final_stats['lever_urls']}")
        logger.info(f"URLs available for scraping: {final_stats['enabled_urls']}")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Find company job board URLs')
    parser.add_argument('--all', action='store_true', help='Run all discovery methods')
    parser.add_argument('--greenhouse', action='store_true', help='Scan Greenhouse directory')
    parser.add_argument('--lever', action='store_true', help='Discover Lever job boards')
    parser.add_argument('--search', action='store_true', help='Perform web searches for job board URLs')
    
    args = parser.parse_args()
    
    finder = CompanyURLFinder()
    
    # Default behavior is to run everything if no specific option is selected
    run_all = args.all or not (args.greenhouse or args.lever or args.search)
    
    if run_all or args.greenhouse:
        finder.scan_greenhouse_directory()
    
    if run_all or args.lever:
        finder.scan_lever_companies()
    
    if run_all or args.search:
        # Greenhouse search terms
        greenhouse_search_terms = [
            "site:boards.greenhouse.io careers",
            "powered by greenhouse"
        ]
        
        for term in greenhouse_search_terms:
            finder.search_companies(term, "greenhouse")
            time.sleep(random.uniform(5, 10))
        
        # Lever search terms
        lever_search_terms = [
            "site:jobs.lever.co careers",
            "powered by lever"
        ]
        
        for term in lever_search_terms:
            finder.search_companies(term, "lever")
            time.sleep(random.uniform(5, 10))
    
    # Print final stats
    stats = finder.get_stats()
    logger.info(f"Company URL Finder complete")
    logger.info(f"Total companies: {stats['total_companies']}")
    logger.info(f"Greenhouse URLs: {stats['greenhouse_urls']}")
    logger.info(f"Lever URLs: {stats['lever_urls']}")
    logger.info(f"URLs available for scraping: {stats['enabled_urls']}") 