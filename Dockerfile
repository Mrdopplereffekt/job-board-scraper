FROM python:3.9-slim

WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create a non-root user to run the application
RUN useradd -m scraper
RUN chown -R scraper:scraper /app
USER scraper

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV CHUNK_SIZE=1
ENV PAGES_TO_SCRAPE_QUERY="select distinct url from company_urls where is_enabled=true;"

# Run the scraper every 24 hours
CMD ["sh", "-c", "python job_board_scraper/run_job_scraper.py && sleep 86400"] 