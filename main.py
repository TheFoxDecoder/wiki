from db import read_tb,write_tb
from indexer import Indexer
from crawl import Crawler

# the loop untill search all the web pages check on the whole domain

crawler = crawler(url)
indexer = Indexer()

Indexer.add(crawler.domain, crawler.url_table)
write_table(schema, indexer.index)


class Wiki:
    def __init__(self,url='https://en.wikipedia.org/wiki/Main_Page'):
        self.url = url
        self.index = Indexer()
        # run loop 
        self.list = self.index.return_link_list()
        write_tb(self.list)
    
    def loop(self):
        current_crawl = self.url_load()
        
        if(len(current_crawl)) == 0: # first in all try
            self.crawler = Crawler(self.url)
            self.links = self.crawler.return_internal()
            self.index.add_entry(self.links)
            
        else: # as in allready called method
            for i in range(current_crawl):
                self.crawler(i)
                self.index.add_entry()


    def url_load(self):
        dt = read_tb()
        if dt != None :
            return self.url
        else:
            # calling the index urls 
            return dt.iloc[:, 1].values