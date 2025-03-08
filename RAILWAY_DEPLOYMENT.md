# Greenhouse Companies Scraper - Railway Deployment

This guide walks you through setting up the Greenhouse Companies Scraper on Railway.

## Overview

This project has two main components:

1. **Company Finder**: Discovers companies using Greenhouse ATS
2. **Job Scraper**: Scrapes job listings from those companies

Both components use the same PostgreSQL database to store data.

## Deployment Steps

### 1. Fork the Repository

You've already completed this step by forking the original repository to:
https://github.com/Mrdopplereffekt/job-board-scraper.git

### 2. Modify the Repository

Replace the following files with the AWS-free versions:

* `job_board_scraper/job_board_scraper/spiders/greenhouse_job_departments_spider.py`
* `job_board_scraper/job_board_scraper/spiders/greenhouse_jobs_outline_spider.py`
* `job_board_scraper/job_board_scraper/settings.py`
* `job_board_scraper/run_job_scraper.py`

Add these new files:
* `Dockerfile` (in the root directory)
* `find_companies.py` (for the company finder)

### 3. Deploy to Railway

You already have a Railway project set up with:
- PostgreSQL database (`greenhouse-db`)
- Node.js service (`greenhouse-scraper`)

We'll modify the Node.js service to use a Python environment instead.

#### Steps:

1. **Connect your GitHub repository**:
   - Go to the Railway dashboard
   - Select your `greenhouse` project
   - Click on the `greenhouse-scraper` service
   - Go to Settings > Connect GitHub repository
   - Select your forked repository `Mrdopplereffekt/job-board-scraper`

2. **Configure the deployment**:
   - Set the service to use the Dockerfile in your repository
   - Verify all environment variables are set:
     - `PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`
     - `HASHIDS_SALT`
     - `PAGES_TO_SCRAPE_QUERY`

3. **Deploy the service**:
   - Railway will automatically build and deploy your service
   - Monitor the deployment logs to ensure everything is working

### 4. Run the Company Finder

To populate your database with companies using Greenhouse:

1. **Connect to your service**:
   - Go to your service in Railway
   - Open the Shell tab

2. **Run the company finder script**:
   ```
   python find_companies.py
   ```

3. **Verify companies were found**:
   - Open the PostgreSQL database in Railway
   - Run a query to check the `company_urls` table:
   ```sql
   SELECT * FROM company_urls LIMIT 10;
   ```

### 5. Run the Job Scraper

1. **In the same Shell**:
   ```
   python job_board_scraper/run_job_scraper.py
   ```

2. **Verify jobs were scraped**:
   - Check the `greenhouse_job_departments` and `greenhouse_jobs_outline` tables:
   ```sql
   SELECT COUNT(*) FROM greenhouse_jobs_outline;
   ```

### 6. Setup Scheduled Runs

For automatic updates:

1. **Use Railway Cron Jobs** (recommended):
   - Create a new Service in your project
   - Select "Cron Job" as the service type
   - Set the schedule (e.g., `0 0 * * *` for daily at midnight)
   - Set the command to run the scripts

2. **Or use the included scheduler in Dockerfile**:
   - The Dockerfile includes a command to run the scraper every 24 hours

## Database Schema

The database contains these main tables:

1. `company_urls`: List of Greenhouse career pages to scrape
2. `greenhouse_companies`: Information about companies using Greenhouse
3. `greenhouse_job_departments`: Departments within each company
4. `greenhouse_jobs_outline`: Job listings from each company

## Modifications Made

This deployment removes the AWS S3 dependencies from the original project:

1. Removed S3 storage of HTML files
2. Simplified the scraper code
3. Added a company finder to build a database of companies using Greenhouse
4. Set up Railway-specific deployment

## Troubleshooting

If you encounter any issues:

1. **Check Logs**: Railway provides detailed logs for each deployment
2. **Verify Database Connection**: Ensure your database credentials are correct
3. **Check Rate Limiting**: If you're being blocked, reduce the scraping frequency
4. **Database Issues**: Verify the tables were created correctly

## Next Steps

- Add custom company URLs to scrape
- Modify the scraper to focus on specific industries
- Build a frontend to view the scraped data
- Export data to CSV or other formats 