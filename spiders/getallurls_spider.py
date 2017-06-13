# -*- coding: utf-8 -*-


import scrapy
import re
import Queue
import time
import w3lib.html
from scrapy.selector import Selector
from urlparse import urljoin
from urlparse import urlparse
from dfsearch.items import DfsearchItem


class GetAllURLsSpider(scrapy.Spider):
    name = "allurls"
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        # domain...
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    urlsQueue = Queue.Queue()

    def __init__(self, *args, **kwargs):
        super(GetAllURLsSpider, self).__init__(*args, **kwargs)
        self.mySQLPipeline = None

    def start_requests(self):

        #self.urlsQueue.put('https://www.hao123.com/')
        self.urlsQueue.put('http://www.dytt8.net')
        yield scrapy.Request(url=self.urlsQueue.get(), callback=self.parse)

    def parse(self, response):
        searchItem = DfsearchItem()
        searchItem['url'] = response.url.replace('\'', '')
        #if self.mySQLPipeline.check_url_exist(url=searchItem.get('url', '')) == False:
        retBody = response.selector.xpath(u'//body').extract_first()
        w3lib.html.replace_escape_chars(retBody, which_ones=('\n','\t','\r'), replace_by=u'')
        searchItem['info'] = w3lib.html.remove_tags(retBody).strip()
        urlObj = urlparse(response.url)
        searchItem['domain'] = urlObj.netloc
        print urlObj.netloc
        yield searchItem

        links = Selector(response).xpath('//a[@href]')
        for link in links:
            rawLink = link.xpath('@href').extract_first()
            joinedURL = urljoin(response.url, rawLink).replace('\'', '')
            if self.regex.match(joinedURL) \
            and self.mySQLPipeline.check_url_exist(url=joinedURL) == False \
            and not (joinedURL in self.urlsQueue.queue):
                self.urlsQueue.put(joinedURL)
            if self.urlsQueue.qsize() > 0:
                yield scrapy.Request(url=self.urlsQueue.get(), callback=self.parseLoop)

    def parseLoop(self, response):
        searchItem = DfsearchItem()
        searchItem['url'] = response.url.replace('\'', '')
        #if self.mySQLPipeline.check_url_exist(url=searchItem.get('url', '')) == False:
        retBody = response.selector.xpath(u'//body').extract_first()
        w3lib.html.replace_escape_chars(retBody, which_ones=('\n','\t','\r'), replace_by=u'')
        searchItem['info'] = w3lib.html.remove_tags(retBody).strip()
        urlObj = urlparse(response.url)
        searchItem['domain'] = urlObj.netloc
        print urlObj.netloc
        yield searchItem

        links = Selector(response).xpath('//a[@href]')
        for link in links:
            rawLink = link.xpath('@href').extract_first()
            joinedURL = urljoin(response.url, rawLink).replace('\'', '')
            if self.regex.match(joinedURL) \
            and self.mySQLPipeline.check_url_exist(url=joinedURL) == False \
            and not (joinedURL in self.urlsQueue.queue):
                self.urlsQueue.put(joinedURL)
            if self.urlsQueue.qsize() > 0:
                yield scrapy.Request(url=self.urlsQueue.get(), callback=self.parseLoop)
