import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import re
import pandas as pd
import os
import numpy as np
from urllib import parse

class spiderName(scrapy.Spider):
    name = "Generic_spider" 
    unified_dict = {} # structure as {'leadId' : [website,email,insta]}
    # within-script-configuration
    custom_settings = {
        'DOWNLOAD_DELAY' : 0.25, #means how much to wait before sending another request
        "USER_AGENT" : "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "RETRY_TIMES": 3,
    }

    # define your pages here
    defined_pages = [
        "/kontakt/",
        "/impressum/"
    ]
    
    # constructore/initializer which gets executes everytime class instantiate
    def __init__(self):
        pass
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(spiderName, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        self.all_data = pd.read_csv("electricions_dataset.csv")
        self.all_data = self.all_data.replace(np.nan,None)
        filtered_rows = self.all_data[self.all_data['contact_numbers'].isnull() & self.all_data['website'].notnull()]
        # filtered_rows = filtered_rows[:2]

        for idx, record in filtered_rows.iterrows():
            website_url = record['website']
            print(website_url)
            for path_param in self.defined_pages:
                url = website_url + path_param
                yield scrapy.Request(url=url, callback=self.parse_page, meta={'id':idx}, dont_filter=True)
    
    def parse_page(self,response):
        telephones = response.css("a[href*='tel:']::attr(href)").extract()
        if not telephones:
            regex = r"\+[-()\d ]{7,}"
            telephones = re.findall(regex, response.text)

        # contact number
        telephones = [tel for tel in telephones if "--" not in tel]
        contact_num = parse.unquote(telephones[0].replace("tel:","").replace("http://","").strip()) if telephones else None
        print(contact_num)

        # update column data
        if not self.all_data.loc[response.meta.get('id'),'contact_numbers']:
            self.all_data.loc[response.meta.get('id'),'contact_numbers'] = contact_num


     # closing the driver
    def spider_closed(self):
        # dump into csv
        self.all_data.to_csv("electricions_dataset.csv",index=False,encoding="utf-8-sig")
        print("DUMPED")
        print("DONE")


process = CrawlerProcess()
process.crawl(spiderName)
process.start()