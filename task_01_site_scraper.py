import scrapy
from scrapy.crawler import CrawlerProcess
import time
import os
import json
from scrapy import signals

class electricSite(scrapy.Spider):
    name = "electric_site"
    url = "https://www.elektrikerjobs.de/baden-wuerttemberg/elektriker"

    # within-script-configuration
    custom_settings = {        
        # configuring output format, export as CSV format
        "FEED_FORMAT" : "csv",
        "FEED_URI" : "electricions_dataset.csv",
        "FEED_EXPORT_ENCODING" : "utf-8-sig",     # for special characters proper encoding
    }

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse)
    
    def parse(self,response):
        all_urls = response.css("div.results>a::attr(href)").extract()
        for url in all_urls:
            yield response.follow(url=url, callback=self.parse_page)
    
    def parse_page(self, response):
        company_name = response.css("address:first-child b::text").extract_first()
        street_address = response.css("address:first-child div[itemprop='streetAddress']::text").extract_first()
        city = "".join(response.css("address:first-child div[class='city'] ::text").extract())
        state = response.css("address:first-child div[class='state']::text").extract_first()
        contact_num = response.css("a.btn-phone::attr(content)").extract()[-1].strip() if response.css("a.btn-phone::attr(content)").extract() else None 
        contact_person = response.css("div[itemProp='applicationContact'] div[itemprop='name']::text").extract_first()
        contact_mail = response.css("a.btn-email::attr(content)").extract()[-1].strip() if response.css("a.btn-email::attr(content)").extract() else None
        website = response.css("a[data-ext-link='website']::attr(href)").extract_first()

        yield {
            "company" : company_name,
            "street_address" : street_address,
            "city" : city,
            "state" : state,
            "contact_person" : contact_person,
            "contact_numbers" : contact_num and "%s"%(contact_num),
            "contact_email" : contact_mail,
            "website" : website
        }


start_time = time.perf_counter()
process = CrawlerProcess()
process.crawl(electricSite)
process.start()

print("Total time taken by script is: ",time.perf_counter() - start_time)