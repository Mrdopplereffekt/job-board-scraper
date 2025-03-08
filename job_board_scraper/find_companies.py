#!/usr/bin/env python3
"""
Company URL Finder for Job Board Scraper

This script discovers companies using various job boards by:
1. Scanning the Greenhouse and Lever job board directories
2. Searching for job board patterns across the web
3. Using an extensive list of known companies to check if they use Greenhouse or Lever
4. Recursive discovery of related companies
5. Industry-specific targeted searches
6. Storing results in the database for the job scraper to use

Usage:
    python find_companies.py [--all] [--greenhouse] [--lever] [--search] [--recursive] [--industry]
"""

import requests
import psycopg2
import os
import time
import random
import logging
import argparse
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0"
]

# Extended list of tech companies to check for job boards
# List expanded to include many more companies across tech, fintech, healthtech, etc.
TOP_TECH_COMPANIES = [
    # Original list
    "Netflix", "Spotify", "Airbnb", "DoorDash", "Stripe", "Uber", "Lyft", "Slack",
    "Pinterest", "Shopify", "Dropbox", "Twitter", "Square", "Zoom", "Coinbase",
    "Twilio", "Gitlab", "Notion", "Atlassian", "Figma", "Canva", "Reddit", "Medium",
    "Asana", "MongoDB", "Datadog", "Cloudflare", "Fastly", "Snowflake", "Databricks",
    
    # Big Tech 
    "Google", "Microsoft", "Amazon", "Apple", "Meta", "Oracle", "Adobe", "IBM", 
    "Dell", "Intel", "Cisco", "SAP", "Salesforce", "VMware", "HP", "Nvidia", "AMD",
    "Tesla", "Qualcomm", "Intuit",
    
    # Fintech
    "Plaid", "Robinhood", "Chime", "SoFi", "Brex", "Affirm", "BlockFi", "Wise", 
    "Klarna", "Revolut", "N26", "Monzo", "Ripple", "Gemini", "CoinList", "Carta",
    "Adyen", "Checkout", "Braintree", "Marqeta", "Chainalysis", "TradeRepublic",
    "Venmo", "Addepar", "Stash", "Wealthfront", "Betterment", "Kraken", "eToro",
    
    # Enterprise SaaS
    "Workday", "ServiceNow", "Okta", "Splunk", "Crowdstrike", "Zendesk", "DocuSign",
    "Airtable", "Box", "Supabase", "Vercel", "Netlify", "Heroku", "DigitalOcean",
    "CircleCI", "HashiCorp", "Auth0", "Segment", "Confluent", "Elastic", "Databricks",
    "Grafana", "PagerDuty", "LaunchDarkly", "Sentry", "Amplitude", "Segment", "Mixpanel",
    
    # Health Tech
    "Oscar", "One Medical", "Color", "Ro", "Zocdoc", "GoodRx", "Hims", "Nurx", "Capsule",
    "Tempus", "Flatiron Health", "Veeva", "K Health", "Sword Health", "Devoted Health",
    "Cedar", "Carbon Health", "Cityblock", "Omada Health", "Notable Health", "Commure",
    
    # Consumer Tech
    "Instacart", "Doordash", "Postmates", "GrubHub", "Getir", "Gorillas", "Flink",
    "Deliveroo", "Glovo", "Wolt", "Bolt", "Lime", "Bird", "Voi", "Tier", "Jump",
    "HelloFresh", "BlueApron", "Casper", "Away", "Allbirds", "Warby Parker", "Glossier",
    "Peloton", "Oura", "Calm", "Headspace", "Tonal", "Mirror", "ClassPass", "Strava",
    "Zwift", "Duolingo", "Quizlet", "Chegg", "Udemy", "Coursera", "edX", "Skillshare",
    
    # Gaming
    "Epic Games", "Riot Games", "Electronic Arts", "Activision Blizzard", "Unity",
    "Roblox", "Zynga", "Niantic", "Supercell", "King", "Ubisoft", "Take-Two", "Valve",
    "Discord", "Twitch", "PUBG", "Mojang", "CCP Games", "Bungie", "Rockstar Games",
    
    # Crypto/Web3
    "Coinbase", "Kraken", "Gemini", "Binance", "FTX", "Solana", "Alchemy", "Polygon",
    "OpenSea", "MetaMask", "ConsenSys", "Uniswap", "Aave", "Compound", "MakerDAO",
    "Dapper Labs", "Sorare", "Immutable", "Phantom", "Axiom Zen", "StarkWare", "Nansen",
    
    # Cybersecurity
    "CrowdStrike", "Darktrace", "SentinelOne", "Tanium", "Snyk", "1Password", "Netskope",
    "Datadog", "Wiz", "Orca Security", "Lacework", "JumpCloud", "KnowBe4", "Arctic Wolf",
    "Axonius", "Dragos", "IronNet", "Exabeam", "Cybereason", "Illumio", "ThreatLocker",
    
    # AI/ML
    "OpenAI", "Anthropic", "Cohere", "Hugging Face", "Scale AI", "Weights & Biases", 
    "Stability AI", "Midjourney", "Jasper", "Runway", "Anthropic", "Inflection AI",
    "Character.AI", "Synthesia", "Moveworks", "Cresta", "Primer", "Deepgram", "H2O.ai",
    "Anyscale", "Galileo", "Labelbox", "Snorkel", "Tecton", "Hex", "Modal", "Paradigm",
    
    # Miscellaneous 
    "Zapier", "Calendly", "Miro", "Loom", "Linear", "Retool", "Ramp", "Brex", "Gusto",
    "Rippling", "Deel", "Remote", "Hopin", "Gong", "Monday", "Figma", "Canva", "Mural",
    "Webflow", "Notion", "Coda", "Frame.io", "Pitch", "Superhuman", "Typeform", "Lattice",
    "Culture Amp", "15Five", "BambooHR", "Personio", "Greenhouse", "Lever", "Ashby",
    "Workable", "SmartRecruiters", "Gem", "Hired", "AngelList", "Vettery", "Triplebyte"
]

# Industry-specific search patterns (company name formats)
INDUSTRY_PATTERNS = {
    'tech': [
        "tech", "software", "technology", "digital", "cloud", "data", "AI", "ML",
        "artificial intelligence", "machine learning", "dev", "development",
        "engineering", "IoT", "internet of things", "quantum", "cybersecurity"
    ],
    'fintech': [
        "fintech", "finance", "banking", "payment", "crypto", "blockchain", "bitcoin",
        "ethereum", "defi", "insuretech", "regtech", "lending", "investing", "wealth",
        "trading", "exchange", "financial", "insurance", "capital", "fund", "asset", 
        "investment", "money", "credit"
    ],
    'healthtech': [
        "health", "healthcare", "biotech", "medical", "medicine", "pharma", "pharmaceutical",
        "genomics", "telehealth", "telemedicine", "health tech", "life science", "clinical",
        "patient", "care", "diagnostic", "therapeutics", "mental health", "wellness"
    ],
    'ecommerce': [
        "ecommerce", "retail", "shop", "store", "marketplace", "commerce", "brand",
        "consumer", "shopping", "direct to consumer", "d2c", "dtc", "e-commerce"
    ],
    'edtech': [
        "education", "learning", "edtech", "teaching", "school", "university", "college",
        "academic", "training", "tutor", "course", "skill", "e-learning"
    ],
    'proptech': [
        "proptech", "real estate", "property", "housing", "rental", "mortgage",
        "construction", "architecture", "building", "home", "apartment", "space"
    ],
    'mobility': [
        "mobility", "transportation", "automotive", "vehicle", "car", "ride", "scooter",
        "bike", "delivery", "logistics", "shipping", "freight", "fleet", "electric",
        "ev", "autonomous", "self-driving", "drone"
    ]
}

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
        
        # Tracking for already checked URLs and companies to avoid duplicates
        self.checked_urls = set()
        self.checked_companies = set()
        
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
                        
                        # Track this URL and company as checked
                        self.checked_urls.add(greenhouse_url)
                        self.checked_companies.add(company_name.lower())
                        
                        # Insert into database
                        self.add_company(company_name, greenhouse_url, "greenhouse")
                        companies_found += 1
                        
                    logger.info(f"Processed Greenhouse companies starting with '{letter}': Found {len(company_links)} companies")
                    
                    # Be nice to Greenhouse servers
                    time.sleep(random.uniform(1, 3))
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
            # Skip if we've already checked this company
            if company.lower() in self.checked_companies:
                continue
                
            # Add variations of company name to check
            company_variations = [
                company.lower().replace(" ", ""),  # nospaceslowercase
                company.lower().replace(" ", "-"),  # hyphenated-lowercase
                company.lower(),  # lowercase with spaces
            ]
            
            for company_slug in company_variations:
                lever_url = f"https://jobs.lever.co/{company_slug}"
                
                # Skip if already checked this URL
                if lever_url in self.checked_urls:
                    continue
                    
                self.checked_urls.add(lever_url)
                
                try:
                    headers = {'User-Agent': self.get_random_user_agent()}
                    response = requests.get(lever_url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        # Insert into database
                        self.add_company(company, lever_url, "lever")
                        companies_found += 1
                        logger.info(f"Found Lever job board for {company}: {lever_url}")
                        break  # We found a valid URL for this company, no need to try other variations
                    
                    # Be nice to Lever servers
                    time.sleep(random.uniform(0.5, 1.5))
                
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
                RETURNING id
                """, (job_board_url, company_name))
                
                result = cursor.fetchone()
                self.conn.commit()
                
                # If a new row was inserted, track this company and URL
                if result:
                    self.checked_companies.add(company_name.lower())
                    self.checked_urls.add(job_board_url)
                    return True
                    
                return False
                
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error adding company {company_name}: {str(e)}")
            return False
    
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
                            # Skip if already checked
                            if link in self.checked_urls:
                                continue
                                
                            # Extract company name from title
                            title_element = result.select_one('h3')
                            company_name = title_element.text if title_element else "Unknown"
                            
                            # Clean up URL to get proper format
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                job_board_url = f"https://boards.greenhouse.io/{company_slug}"
                                
                                # Add to database and tracking
                                self.checked_urls.add(job_board_url)
                                added = self.add_company(company_name, job_board_url, "greenhouse")
                                if added:
                                    companies_found += 1
                        
                        elif board_type == 'lever' and 'jobs.lever.co' in link:
                            # Skip if already checked
                            if link in self.checked_urls:
                                continue
                                
                            # Extract company name from title
                            title_element = result.select_one('h3')
                            company_name = title_element.text if title_element else "Unknown"
                            
                            # Clean up URL to get proper format
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                job_board_url = f"https://jobs.lever.co/{company_slug}"
                                
                                # Add to database and tracking
                                self.checked_urls.add(job_board_url)
                                added = self.add_company(company_name, job_board_url, "lever")
                                if added:
                                    companies_found += 1
            else:
                logger.warning(f"Search request failed with status code: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error searching for companies: {str(e)}")
        
        logger.info(f"Found {companies_found} companies via search for {board_type}")
        return companies_found
    
    def recursive_discovery(self, seed_urls=None, max_depth=2):
        """
        Discover new companies by recursively checking for related companies
        on job board pages and company websites
        """
        logger.info("Starting recursive company discovery...")
        
        if not seed_urls:
            # Get some existing companies from the database as seeds
            with self.conn.cursor() as cursor:
                cursor.execute("""
                SELECT url FROM company_urls 
                WHERE is_enabled = TRUE 
                LIMIT 50
                """)
                
                seed_urls = [row[0] for row in cursor.fetchall()]
        
        companies_found = 0
        url_queue = [(url, 0) for url in seed_urls]  # (URL, depth)
        
        while url_queue:
            current_url, depth = url_queue.pop(0)
            
            # Skip if we've reached max depth
            if depth >= max_depth:
                continue
                
            # Skip if already checked
            if current_url in self.checked_urls:
                continue
                
            self.checked_urls.add(current_url)
            
            try:
                logger.info(f"Recursively checking: {current_url} (depth {depth})")
                headers = {'User-Agent': self.get_random_user_agent()}
                response = requests.get(current_url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 1. Look for "similar companies" or "customers" sections
                    potential_company_links = []
                    
                    # Find links containing "careers", "jobs", "hiring", etc.
                    job_related_links = soup.find_all('a', href=lambda href: href and any(
                        keyword in href.lower() for keyword in ['career', 'job', 'hiring', 'work', 'position']
                    ))
                    
                    for link in job_related_links:
                        href = link.get('href')
                        if not href:
                            continue
                            
                        # Make absolute URL if relative
                        if href.startswith('/'):
                            href = urljoin(current_url, href)
                            
                        # Check if it's a job board URL
                        if 'boards.greenhouse.io' in href or 'jobs.lever.co' in href:
                            potential_company_links.append(href)
                    
                    # Process discovered links
                    for link in potential_company_links:
                        if 'boards.greenhouse.io' in link:
                            # Extract company slug
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                company_name = company_slug.replace('-', ' ').title()
                                job_board_url = f"https://boards.greenhouse.io/{company_slug}"
                                
                                # Add to database
                                added = self.add_company(company_name, job_board_url, "greenhouse")
                                if added:
                                    companies_found += 1
                                    # Add to queue for next iteration
                                    if depth + 1 < max_depth:
                                        url_queue.append((job_board_url, depth + 1))
                                    
                        elif 'jobs.lever.co' in link:
                            # Extract company slug
                            parsed_url = urlparse(link)
                            path_parts = parsed_url.path.split('/')
                            
                            if len(path_parts) > 1 and path_parts[1]:
                                company_slug = path_parts[1].strip()
                                company_name = company_slug.replace('-', ' ').title()
                                job_board_url = f"https://jobs.lever.co/{company_slug}"
                                
                                # Add to database
                                added = self.add_company(company_name, job_board_url, "lever")
                                if added:
                                    companies_found += 1
                                    # Add to queue for next iteration
                                    if depth + 1 < max_depth:
                                        url_queue.append((job_board_url, depth + 1))
                    
                    # Be nice to servers
                    time.sleep(random.uniform(1, 2))
            
            except Exception as e:
                logger.error(f"Error in recursive discovery for {current_url}: {str(e)}")
        
        logger.info(f"Recursive discovery completed. Found {companies_found} new companies.")
        return companies_found
    
    def industry_specific_search(self):
        """
        Perform industry-specific searches to find more companies
        using targeted queries
        """
        logger.info("Starting industry-specific company search...")
        total_companies_found = 0
        
        for industry, keywords in INDUSTRY_PATTERNS.items():
            logger.info(f"Searching {industry} industry...")
            industry_companies_found = 0
            
            for keyword in keywords:
                # Search for Greenhouse companies in this industry
                gh_query = f"{keyword} companies site:boards.greenhouse.io"
                companies_found = self.search_companies(gh_query, "greenhouse", max_results=30)
                industry_companies_found += companies_found
                
                # Be nice to search engines
                time.sleep(random.uniform(2, 4))
                
                # Search for Lever companies in this industry
                lever_query = f"{keyword} companies site:jobs.lever.co"
                companies_found = self.search_companies(lever_query, "lever", max_results=30)
                industry_companies_found += companies_found
                
                # Be nice to search engines
                time.sleep(random.uniform(2, 4))
                
                # For more targeted results, try industry + job board provider
                gh_provider_query = f"{keyword} greenhouse job board"
                companies_found = self.search_companies(gh_provider_query, "greenhouse", max_results=20)
                industry_companies_found += companies_found
                
                # Be nice to search engines
                time.sleep(random.uniform(2, 4))
                
                lever_provider_query = f"{keyword} lever job board"
                companies_found = self.search_companies(lever_provider_query, "lever", max_results=20)
                industry_companies_found += companies_found
                
                # Be nice to search engines
                time.sleep(random.uniform(2, 4))
            
            logger.info(f"Found {industry_companies_found} companies in {industry} industry")
            total_companies_found += industry_companies_found
        
        logger.info(f"Industry-specific search completed. Found {total_companies_found} companies total.")
        return total_companies_found
    
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
        
        # 1. Scan Greenhouse directory
        self.scan_greenhouse_directory()
        
        # 2. Discover Lever job boards
        self.scan_lever_companies()
        
        # 3. Search for Greenhouse companies using different search terms
        greenhouse_search_terms = [
            "site:boards.greenhouse.io careers",
            "site:boards.greenhouse.io jobs",
            "apply through greenhouse io",
            "powered by greenhouse",
            "apply for this job greenhouse",
            "we use greenhouse",
            "greenhouse applicant tracking",
            "greenhouse ats jobs",
            "using greenhouse for jobs",
            "careers greenhouse",
            "join our team greenhouse",
            "open positions greenhouse"
        ]
        
        for term in greenhouse_search_terms:
            self.search_companies(term, "greenhouse")
            time.sleep(random.uniform(3, 6))  # Avoid rate limiting
        
        # 4. Search for Lever companies
        lever_search_terms = [
            "site:jobs.lever.co careers",
            "site:jobs.lever.co apply",
            "apply through lever.co",
            "powered by lever",
            "apply for this job lever",
            "we use lever",
            "lever applicant tracking",
            "lever ats jobs",
            "using lever for jobs",
            "careers lever",
            "join our team lever",
            "open positions lever"
        ]
        
        for term in lever_search_terms:
            self.search_companies(term, "lever")
            time.sleep(random.uniform(3, 6))  # Avoid rate limiting
        
        # 5. Perform recursive discovery
        self.recursive_discovery(max_depth=2)
        
        # 6. Perform industry-specific searches
        self.industry_specific_search()
        
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
    parser.add_argument('--recursive', action='store_true', help='Perform recursive discovery of related companies')
    parser.add_argument('--industry', action='store_true', help='Perform industry-specific searches')
    
    args = parser.parse_args()
    
    finder = CompanyURLFinder()
    
    # Default behavior is to run everything if no specific option is selected
    run_all = args.all or not (args.greenhouse or args.lever or args.search or args.recursive or args.industry)
    
    if run_all or args.greenhouse:
        finder.scan_greenhouse_directory()
    
    if run_all or args.lever:
        finder.scan_lever_companies()
    
    if run_all or args.search:
        # Greenhouse search terms
        greenhouse_search_terms = [
            "site:boards.greenhouse.io careers",
            "powered by greenhouse",
            "greenhouse ats jobs"
        ]
        
        for term in greenhouse_search_terms:
            finder.search_companies(term, "greenhouse")
            time.sleep(random.uniform(2, 5))
        
        # Lever search terms
        lever_search_terms = [
            "site:jobs.lever.co careers",
            "powered by lever",
            "lever ats jobs"
        ]
        
        for term in lever_search_terms:
            finder.search_companies(term, "lever")
            time.sleep(random.uniform(2, 5))
    
    if run_all or args.recursive:
        finder.recursive_discovery()
        
    if run_all or args.industry:
        finder.industry_specific_search()
    
    # Print final stats
    stats = finder.get_stats()
    logger.info(f"Company URL Finder complete")
    logger.info(f"Total companies: {stats['total_companies']}")
    logger.info(f"Greenhouse URLs: {stats['greenhouse_urls']}")
    logger.info(f"Lever URLs: {stats['lever_urls']}")
    logger.info(f"URLs available for scraping: {stats['enabled_urls']}") 