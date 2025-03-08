#!/usr/bin/env python3
"""
Greenhouse Company Finder

This script discovers companies using Greenhouse ATS by:
1. Scanning the Greenhouse job boards directory
2. Searching for "boards.greenhouse.io" patterns across the web
3. Storing results in the same database used by the job scraper
"""

import requests
import psycopg2
import os
import time
import random
import logging
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlparse

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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0"
]

class GreenhouseCompanyFinder:
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
        
        # Create companies table if it doesn't exist
        self.init_database()
        
    def init_database(self):
        """Initialize the database schema if it doesn't exist"""
        with self.conn.cursor() as cursor:
            # Create company_urls table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_urls (
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL UNIQUE,
                is_enabled BOOLEAN DEFAULT TRUE,
                company_name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS greenhouse_companies (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255),
                domain VARCHAR(255),
                greenhouse_url VARCHAR(255) UNIQUE,
                job_count INTEGER DEFAULT 0,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
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
        
        # We'll simulate browsing through alphabet pages
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
                        self.add_company(company_name, greenhouse_url)
                        companies_found += 1
                        
                    logger.info(f"Processed companies starting with '{letter}': Found {len(company_links)} companies")
                    
                    # Be nice to Greenhouse servers
                    time.sleep(random.uniform(3, 5))
                else:
                    logger.warning(f"Failed to get companies for letter '{letter}'. Status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error scanning Greenhouse directory for letter '{letter}': {str(e)}")
        
        logger.info(f"Completed Greenhouse directory scan. Found {companies_found} companies.")
        return companies_found
    
    def add_company(self, company_name, greenhouse_url):
        """Add a company to the database"""
        try:
            domain = None
            
            # Extract domain from URL
            parsed_url = urlparse(greenhouse_url)
            if parsed_url.netloc:
                domain = parsed_url.netloc
                
            # Add to greenhouse_companies table
            with self.conn.cursor() as cursor:
                cursor.execute("""
                INSERT INTO greenhouse_companies (company_name, domain, greenhouse_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (greenhouse_url) DO NOTHING
                """, (company_name, domain, greenhouse_url))
                
                # Also add to company_urls table for the scraper
                cursor.execute("""
                INSERT INTO company_urls (url, company_name)
                VALUES (%s, %s)
                ON CONFLICT (url) DO NOTHING
                """, (greenhouse_url, company_name))
                
                self.conn.commit()
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding company {company_name}: {str(e)}")
    
    def search_companies(self, query, max_results=50):
        """Search for companies using Greenhouse"""
        
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
                        
                        # Extract Greenhouse URL if it exists
                        if 'boards.greenhouse.io' in link:
                            # Extract company name from title
                            title_element = result.select_one('h3')
                            company_name = title_element.text if title_element else "Unknown"
                            
                            # Clean up URL to get proper format
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                greenhouse_url = f"https://boards.greenhouse.io/{company_slug}"
                                
                                # Add to database
                                self.add_company(company_name, greenhouse_url)
                                companies_found += 1
            else:
                logger.warning(f"Search request failed with status code: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error searching for companies: {str(e)}")
        
        logger.info(f"Found {companies_found} companies via search")
        return companies_found
    
    def get_stats(self):
        """Get statistics about the companies database"""
        with self.conn.cursor() as cursor:
            # Count total companies
            cursor.execute("SELECT COUNT(*) FROM greenhouse_companies")
            total_companies = cursor.fetchone()[0]
            
            # Count enabled URLs for scraping
            cursor.execute("SELECT COUNT(*) FROM company_urls WHERE is_enabled = TRUE")
            enabled_urls = cursor.fetchone()[0]
            
            return {
                "total_companies": total_companies,
                "enabled_urls": enabled_urls
            }
    
    def run(self):
        """Run the complete company finder process"""
        logger.info("Starting Greenhouse Company Finder")
        
        # Get initial stats
        initial_stats = self.get_stats()
        
        # Scan Greenhouse directory
        self.scan_greenhouse_directory()
        
        # Search for companies using different search terms
        search_terms = [
            "site:boards.greenhouse.io careers",
            "site:boards.greenhouse.io jobs",
            "apply through greenhouse io",
            "powered by greenhouse",
            "apply for this job greenhouse"
        ]
        
        for term in search_terms:
            self.search_companies(term)
            time.sleep(random.uniform(10, 15))  # Avoid rate limiting
        
        # Get final stats
        final_stats = self.get_stats()
        
        logger.info(f"Greenhouse Company Finder complete")
        logger.info(f"Initial companies: {initial_stats['total_companies']}")
        logger.info(f"Final companies: {final_stats['total_companies']}")
        logger.info(f"New companies found: {final_stats['total_companies'] - initial_stats['total_companies']}")
        logger.info(f"URLs available for scraping: {final_stats['enabled_urls']}")
        

if __name__ == "__main__":
    finder = GreenhouseCompanyFinder()
    finder.run() 