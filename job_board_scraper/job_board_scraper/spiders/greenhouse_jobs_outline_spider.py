import scrapy
import time
import os
from dotenv import load_dotenv
from job_board_scraper.items import GreenhouseJobsOutlineItem
from job_board_scraper.utils import general as util
from job_board_scraper.spiders.greenhouse_job_departments_spider import (
    GreenhouseJobDepartmentsSpider,
)
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings
from datetime import datetime
import psycopg2
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError, TimeoutError

load_dotenv()

class GreenhouseJobsOutlineSpider(GreenhouseJobDepartmentsSpider):
    name = "greenhouse_jobs_outline"
    allowed_domains = ["boards.greenhouse.io", "job-boards.greenhouse.io"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_id = kwargs.pop("spider_id", 2)
        self.logger.info(f"Initialized Spider, {self.html_source}")
        self.page_number = 1
        # Add a retry counter
        self.retry_counts = {}
        self.max_retries = 3  # Only disable after 3 failures

    def start_requests(self):
        # Override the parent's start_requests to add error handling
        yield scrapy.Request(
            url=self.url, 
            callback=self.parse,
            errback=self.errback_httpbin,
            dont_filter=True
        )

    def errback_httpbin(self, failure):
        """Handle different types of errors and mark URL as disabled if it's a 404"""
        # Log all failures
        self.logger.error(f"Error processing {self.url}: {repr(failure)}")
        
        if failure.check(HttpError):
            # Get the response
            response = failure.value.response
            self.logger.error(f"HttpError on {response.url} with status {response.status}")
            
            # If it's a 404, mark the URL as disabled in the database immediately
            if response.status == 404:
                self.mark_url_as_disabled()
                
        elif failure.check(DNSLookupError, TimeoutError, TCPTimedOutError):
            # For transient errors, implement retry logic
            request = failure.request
            error_type = "DNSLookupError" if failure.check(DNSLookupError) else "TimeoutError"
            self.logger.error(f"{error_type} on {request.url}")
            
            # Increment retry counter for this URL
            if self.url not in self.retry_counts:
                self.retry_counts[self.url] = 1
            else:
                self.retry_counts[self.url] += 1
            
            # Only mark as disabled after multiple retries
            if self.retry_counts[self.url] >= self.max_retries:
                self.logger.warning(f"URL {self.url} failed {self.max_retries} times, marking as disabled")
                self.mark_url_as_disabled()
            else:
                self.logger.info(f"Will retry URL {self.url} later (attempt {self.retry_counts[self.url]} of {self.max_retries})")
    
    def mark_url_as_disabled(self):
        """Mark the URL as disabled in the database"""
        try:
            # Connect to the database
            connection = psycopg2.connect(
                host=os.environ.get("PG_HOST"),
                user=os.environ.get("PG_USER"),
                password=os.environ.get("PG_PASSWORD"),
                dbname=os.environ.get("PG_DATABASE"),
            )
            
            cursor = connection.cursor()
            
            # Use self.url instead of self.html_source as it's more reliable
            cursor.execute(
                "UPDATE company_urls SET is_enabled=false, updated_at=CURRENT_TIMESTAMP WHERE url=%s RETURNING id;",
                (self.url,)
            )
            
            # Get the ID of the updated row if any
            result = cursor.fetchone()
            
            if result:
                self.logger.info(f"Marked URL {self.url} as disabled (ID: {result[0]})")
            else:
                self.logger.warning(f"URL {self.url} not found in database")
            
            connection.commit()
            cursor.close()
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Error updating database: {str(e)}")

    def get_department_ids(self, job_post):
        stratified_selector = Selector(text=job_post.get(), type="html")

        primary_department = stratified_selector.xpath(
            "//*[starts-with(name(), 'h')]/text()"
        ).get()

        department_ids = self.company_name + "_" + primary_department

        job_openings = stratified_selector.xpath("//td[@class='cell']")

        return department_ids, job_openings

    def parse_job_boards_prefix(self, i, j, department_ids, opening):
        il = ItemLoader(
            item=GreenhouseJobsOutlineItem(),
            selector=Selector(text=opening.get(), type="html"),
        )

        il.add_value("department_ids", department_ids)
        il.add_xpath("opening_link", "//a/@href")
        il.add_xpath("opening_title", "//p[contains(@class, 'body--medium')]/text()")
        il.add_xpath("location", "//p[contains(@class, 'body--metadata')]/text()")

        il.add_value(
            "id",
            self.determine_row_id(
                int(str(i * 100) + str(j * 100) + str(self.page_number))
            ),
        )
        il.add_value("created_at", self.created_at)
        il.add_value("updated_at", self.updated_at)
        il.add_value("source", self.html_source)
        il.add_value("run_hash", self.run_hash)

        return il

    def parse(self, response):
        response_html = self.finalize_response(response)
        selector = Selector(text=response_html, type="html")
        if self.careers_page_url.split(".")[0].split("/")[-1] == "job-boards":
            job_posts = selector.xpath("//div[(@class='job-posts')]")
            for i, job_post in enumerate(job_posts):
                department_ids, job_openings = self.get_department_ids(job_post)
                for j, opening in enumerate(job_openings):
                    il = self.parse_job_boards_prefix(i, j, department_ids, opening)
                    print(
                        il.load_item().get("opening_title"),
                        il.load_item().get("id"),
                    )
                    yield il.load_item()
            if len(job_posts) != 0:
                self.page_number += 1
                yield response.follow(
                    url=self.careers_page_url + f"?page={self.page_number}",
                    callback=self.parse,
                )

        else:
            job_openings = selector.xpath('//div[@class="opening"]')

            for i, opening in enumerate(job_openings):
                il = ItemLoader(
                    item=GreenhouseJobsOutlineItem(),
                    selector=Selector(text=opening.get(), type="html"),
                )
                self.logger.info(f"Parsing row {i+1}, {self.company_name} {self.name}")
                nested = il.nested_xpath('//div[@class="opening"]')

                nested.add_xpath("department_ids", "@department_id")
                nested.add_xpath("office_ids", "@office_id")
                il.add_xpath("opening_link", "//a/@href")
                il.add_xpath("opening_title", "//a/text()")
                il.add_xpath("location", "//span/text()")

                il.add_value("id", self.determine_row_id(i))
                il.add_value("created_at", self.created_at)
                il.add_value("updated_at", self.updated_at)
                il.add_value("source", self.html_source)
                il.add_value("run_hash", self.run_hash)

                yield il.load_item()
