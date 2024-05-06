'''
    Scrap all the ULRs in the WEB


'''
from plistlib import load
import requests as rs
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass 
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

@dataclass
class URL_Table:
    URL_type: bool # internal 1 external 0
    url: str
    tot: int 

schema = pa.schema([
    ('hash_id', pa.int64()),
    ('url', pa.string())
])

class Crewl:
    def __init__(self,url):
        self.urls = self.get_all_urls(url)
        self.domain = self.get_domain(url) # returns in as hash-number
        self.url_table = self.url_devide()
        
        self.table = URL_Table(URL_type=0, url="", tot=0)
    
    def get_domain(self,url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return hash(domain)

    def get_all_urls(self, url):  # Added self parameter
        # Send a GET request to the URL
        response = rs.get(url)
    
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all anchor tags
            links = soup.find_all('a')
            
            # Extract the href attribute from each anchor tag
            urls = [link.get('href') for link in links]
            
            # Filter out None values and empty strings
            urls = [urljoin(url, u) for u in urls if u and u.strip()]
            
            return urls
        else:
            # If the request was unsuccessful, print an error message
            print("Failed to fetch URL:", url)
            return []
        
    def url_devide(self):
        for url in self.urls:
            if self.domain == self.get_domain(url):
                self.table.URL_type = 1
                self.table.url = url
                self.table.tot += 1
            else:
                pass 
    
class Indexer:
    def __init__(self):
        self.index = {}
    
    def add(self, key, value):
        self.index[key] = value
    
    def get(self, key):
        return self.index.get(key)

def load_table():
    pq_instance = pq.ParquetFile('dev/indexer.parquet')
    # Read Parquet file and Convert table to Pandas DataFrame (optional)
    return pq_instance.read().to_pandas()

def duplicateCheck(url_hash):
    indexer = Indexer()
    if indexer.get(url_hash) is None:
        return False
    else:
        return True

def write_table(schema, data):
    # Create PyArrow table
    table = pa.Table.from_pydict(data, schema=schema)

    # Write the table to a Parquet file
    pq.write_table(table, 'dev/indexer.parquet')

# Example usage
url = 'https://en.wikipedia.org/wiki/Main_Page'
crawler = Crewl(url)
indexer = Indexer()
indexer.add(crawler.domain, crawler.url_table)
write_table(schema, indexer.index)
