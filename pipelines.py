# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import hashlib
import MySQLdb
import Queue
from scrapy.conf import settings
from dfsearch.items import DfsearchItem
from elasticsearch import Elasticsearch, helpers


class MySQLPipeline(object):
    itemQueue = Queue.Queue()

    def __init__(self):
        self.conn = MySQLdb.connect(
            settings['MYSQL_HOST'],
            settings['MYSQL_USER'],
            settings['MYSQL_PSWD'],
            settings['MYSQL_DBNAME'],
            charset=settings['MYSQL_CHARSET'],
            use_unicode=settings['MYSQL_UNICODE']
        )
        self.cursor = self.conn.cursor()

        self.es = Elasticsearch([
            {'host': settings['ES_HOST']}
        ])

    def check_url_exist(self, url):
        if url == None:
            return True
        try:
            checkQuery = (
                "SELECT COUNT(*) FROM pagedurl where url = '%s'") % url
            self.cursor.execute(checkQuery)
            if int(self.cursor.fetchone()[0]) > 0:
                return True
            else:
                return False
        except MySQLdb.Error, e:
            print 'DB Error %d: %s' % (e.args[0], e.args[1])
            print checkQuery
            print url

    def open_spider(self, spider):
        spider.mySQLPipeline = self

    def process_item(self, item, spider):
        if isinstance(item, DfsearchItem):
            try:
                self.cursor.execute("INSERT INTO pagedurl (url, domain) \
                VALUES ('%s', '%s')" % (item.get('url', ''), item.get('domain', '')))
                self.conn.commit()
            except MySQLdb.Error, e:
                print 'DB Error %d: %s' % (e.args[0], e.args[1])

            self.itemQueue.put(item)
            if self.itemQueue.qsize() > 100:
                itemArray = []
                while self.itemQueue.qsize() > 0:
                    qItem = self.itemQueue.get()
                    doc = {
                        '_index': 'dfsearch',
                        '_type': 'web',
                        '_id': hashlib.md5(item.get('url', '')).hexdigest(),
                        'doc': {
                            'url': qItem.get('url', ''),
                            'info': qItem.get('info', '')
                        }
                    }
                    itemArray.append(doc)
                helpers.bulk(self.es, itemArray)
        return item
