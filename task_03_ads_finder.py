import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
import re
import pandas as pd
import os
import numpy as np
from urllib import parse
import json 

class spiderName(scrapy.Spider):
    name = "Generic_spider" 
    unified_dict = {} # structure as {'leadId' : [website,email,insta]}
    # within-script-configuration
    custom_settings = {
        'DOWNLOAD_DELAY' : 0.25, #means how much to wait before sending another request
        # "USER_AGENT" : "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "RETRY_TIMES": 3,
    }
    headers = {
        'authority': 'www.facebook.com',
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.facebook.com',
        'referer': 'https://www.facebook.com/ads/library/?active_status=all&ad_type=employment_ads&country=DE&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped&media_type=all',
        'sec-ch-ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'x-fb-lsd': 'AVpZsWJrdUI',
        'accept-language': 'en',
    }
    payload = '__user=0&__a=1&__req=9&__ccg=EXCELLENT&lsd=AVpZsWJrdUI'
    
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
        # adding status column
        self.all_data['FB_status'] = [np.nan]*len(self.all_data.index)
        self.all_data['Ad_ids'] = [np.nan]*len(self.all_data.index)

        for idx, record in self.all_data.iterrows():
            company = record['company']
            url_encoded = parse.quote(company)

            print("Initiated for compnay: ",url_encoded)
            yield scrapy.Request(url=f"https://www.facebook.com/ads/library/async/search_typeahead/?ad_type=employment_ads&country=DE&q={url_encoded}",headers=self.headers, method="POST", body=str(self.payload), callback=self.parse_search, dont_filter=True, meta={'id':idx})
    
    def parse_search(self,response):
        """ function to parse search result """
        response_text = response.text.replace("for (;;);","")
        parsed_data = json.loads(response_text)

        search_results = parsed_data.get("payload").get("pageResults")
        if search_results:
            yield scrapy.Request(url=f"https://www.facebook.com/ads/library/async/search_ads/?count=30&active_status=all&ad_type=employment_ads&countries\\[0\\]=DE&view_all_page_id={search_results[0].get('id')}&media_type=all&search_type=page", headers=self.headers, method="POST",body=str(self.payload),callback=self.parse_page,dont_filter=True,meta={'id':response.meta.get('id')})
        else:
            self.all_data.loc[response.meta.get('id'),'FB_status'] = 'No'

    def parse_page(self, response):
        """ function to parse ad page """
        response_text = response.text.replace("for (;;);","")
        parsed_data = json.loads(response_text)

        search_results = parsed_data.get("payload").get("results")
        if search_results:
            self.all_data.loc[response.meta.get('id'),'FB_status'] = 'Yes'
            self.all_data.loc[response.meta.get('id'),'Ad_ids'] = str([record[0].get('adArchiveID') for record in search_results])
        else:
            self.all_data.loc[response.meta.get('id'),'FB_status'] = 'No'

     # closing the driver
    def spider_closed(self):
        # dump into csv
        self.all_data.to_csv("electricions_dataset.csv",index=False,encoding="utf-8-sig")
        print("DUMPED")


process = CrawlerProcess()
process.crawl(spiderName)
process.start()