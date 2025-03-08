# import logging
import scrapy
import time
import os
from dotenv import load_dotenv
from job_board_scraper.items import GreenhouseJobDepartmentsItem
from job_board_scraper.utils import general as util
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.project import get_project_settings
from datetime import datetime

load_dotenv()
# logger = logging.getLogger("logger")


class GreenhouseJobDepartmentsSpider(scrapy.Spider):
    name = "greenhouse_job_departments"
    allowed_domains = ["boards.greenhouse.io", "job-boards.greenhouse.io"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_id = kwargs.pop("spider_id", 1)
        self.careers_page_url = kwargs.pop("careers_page_url")
        self.run_hash = kwargs.pop("run_hash")
        self.url_id = kwargs.pop("url_id", 0)
        self.html_source = (
            self.careers_page_url[:-1]
            if self.careers_page_url[-1] == "/"
            else self.careers_page_url
        )
        self.settings = get_project_settings()
        self.current_time = time.time()
        self.page_number = 1  # default
        self.updated_at = int(self.current_time)
        self.created_at = int(self.current_time)
        self.current_date_utc = datetime.utcfromtimestamp(self.current_time).strftime(
            "%Y-%m-%d"
        )
        self.logger.info(f"Initialized Spider, {self.html_source}")

    @property
    def url(self):
        return self.html_source

    @property
    def company_name(self):
        # Different format for embedded html
        if "for=" in self.html_source:
            return self.html_source.split("for=")[-1]
        # Traditional format
        return self.html_source.split("/")[-1].split("?")[0]

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse)

    def determine_row_id(self, i):
        return util.hash_ids.encode(
            self.spider_id, i, self.url_id, int(self.created_at)
        )

    def finalize_response(self, response):
        return response.text

    # Greenhouse has exposed a new URL with different features for scraping for some companies
    def parse_job_boards_prefix(self, i, department):
        il = ItemLoader(
            item=GreenhouseJobDepartmentsItem(),
            selector=Selector(text=department.get(), type="html"),
        )
        self.logger.info(f"Parsing row {i+1}, {self.company_name}, {self.name}")

        il.add_value("department_id", self.company_name + "_" + department.get())
        il.add_value("department_name", department.get())
        il.add_value("department_category", "level-0")

        il.add_value("id", self.determine_row_id(i))
        il.add_value("created_at", self.created_at)
        il.add_value("updated_at", self.updated_at)

        il.add_value("source", self.html_source)
        il.add_value("company_name", self.company_name)
        il.add_value("run_hash", self.run_hash)

        return il

    def parse(self, response):
        response_html = self.finalize_response(response)
        selector = Selector(text=response_html, type="html")
        if self.careers_page_url.split(".")[0].split("/")[-1] == "job-boards":
            all_departments = selector.xpath(
                "//div[(@class='job-posts')]/*[starts-with(name(), 'h')]/text()"
            )
            for i, department in enumerate(all_departments):
                il = self.parse_job_boards_prefix(i, department)
                yield il.load_item()
            if len(all_departments) != 0:
                self.page_number += 1
                yield response.follow(
                    self.careers_page_url + f"?page={self.page_number}", self.parse
                )

        else:
            all_departments = selector.xpath('//section[contains(@class, "level")]')

            for i, department in enumerate(all_departments):
                il = ItemLoader(
                    item=GreenhouseJobDepartmentsItem(),
                    selector=Selector(text=department.get(), type="html"),
                )
                dept_loader = il.nested_xpath(
                    f"//section[contains(@class, 'level')]/*[starts-with(name(), 'h')]"
                )
                self.logger.info(f"Parsing row {i+1}, {self.company_name}, {self.name}")

                dept_loader.add_xpath("department_id", "@id")
                dept_loader.add_xpath("department_name", "text()")
                il.add_xpath(
                    "department_category", "//section[contains(@class, 'level')]/@class"
                )

                il.add_value("id", self.determine_row_id(i))
                il.add_value("created_at", self.created_at)
                il.add_value("updated_at", self.updated_at)

                il.add_value("source", self.html_source)
                il.add_value("company_name", self.company_name)
                il.add_value("run_hash", self.run_hash)

                yield il.load_item()
