import hashlib
import pandas as pd
import pyarrow.parquet as pq
import requests as rs
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class Crawler:
    def __init__(self):
        pass
    
    @staticmethod
    def get_all_urls(url):
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

class Indexer:
    def __init__(self):
        self.index = pd.DataFrame(columns=['hash_id', 'url'])
        self.current_id = 1
    
    def generate_hash_id(self, url):
        return hashlib.sha256(url.encode()).hexdigest()

    def add_entry(self, url):
        hash_id = self.generate_hash_id(url)
        if not self.search_by_hash_id(hash_id):
            self.index = self.index.append({'hash_id': hash_id, 'url': url}, ignore_index=True)
            self.current_id += 1
        else:
            pass 
    
    def search_by_hash_id(self, hash_id):
        return not self.index[self.index['hash_id'] == hash_id].empty
    
    def return_index(self):
        return self.index

def write_tb(df, table_name='indexer'):
    try:
        table_name += ".parquet"
        df.to_parquet(table_name)
        print(f"Table '{table_name}' successfully written.")
    except Exception as e:
        print(f"Error occurred while writing table '{table_name}': {e}")

class Wiki:
    def __init__(self,url='https://en.wikipedia.org/wiki/Main_Page'):
        self.url = url
        self.crawler = Crawler()
        self.indexer = Indexer()
        self.map_urls()
    
    def map_urls(self):
        urls_to_map = [self.url]
        while urls_to_map:
            current_url = urls_to_map.pop(0)
            urls = self.crawler.get_all_urls(current_url)
            internal_urls = [url for url in urls if self.is_internal(url)]
            for url in internal_urls:
                self.indexer.add_entry(url)
                urls_to_map.append(url)
        write_tb(self.indexer.return_index())
    
    def is_internal(self, url):
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        return hash(domain) == self.indexer.generate_hash_id(self.url)

if __name__ == "__main__":
    wiki = Wiki("https://www.getclearspace.com/")
