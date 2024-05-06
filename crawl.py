'''
    Scrap all the ULRs in the WEB


'''
from plistlib import load
import requests as rs
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

import pandas as pd



class Crawler:
    def __init__(self,url):
        self.url = url
        self.urls = self.get_all_urls(self.url)
        self.domain = self.get_domain(self.url) # returns in as hash-number
        
        self.internal = []
        self.tot = 0

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
                self.internal.append(url)
            else:
                pass 

    def return_internal(self):
        return self.internal

