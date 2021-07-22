"""
This file is for pulling textual data from a sample of 100 websites from csv/input.csv
to have enough data with which to write regex tests. Output will be saved to sites_data/initial.csv

Set up redis server:
$redis-server

Set up celery worker:
$celery -A data_generator worker -P eventlet --concurrency 1000 --purge

Run file:
$python data_generator.py
"""

import pandas as pd
from celery import Celery
from celery.result import AsyncResult
from bs4 import BeautifulSoup
from decouple import config
import requests
import time


# load data
reader = pd.read_csv('sites_list/main.csv', iterator=True)
df = reader.get_chunk(5)


# initiate celery instance
app = Celery('data_generator', broker=config('BROKER_URL'))


# define scraper
class QuickScrape:
    """Quick scraper that goes 2 links deep into one site and collects all text in result attribute"""

    # initialize scraper with href1 (initial (1st) link)
    def __init__(self, href1):
        self.href1 = href1
        self.results = []
    
    # href parser using bs4
    def find_href(self, soup):
        hrefs = []
        for tag in soup.find_all('a', href=True):
            hrefs.append(tag.get('href'))
        return hrefs

    # all together now
    def parse(self, href):
        # account for bad links and prevent scraper following external links
        if self.href1 not in str(href):
            print(f"Bad/external link: {href}")
        else:
            # cooldown
            time.sleep(2)
            response = requests.get(href, timeout=5)
            # account for bad requests
            if int(response.status_code) > 400:
                print(f"Bad request for {href}: {response.status_code}")
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                # save results
                self.results.append(soup.get_text(strip=True))
                # return list of links
                return self.find_href(soup)

    # iterate parse function 2 links deep
    def execute(self):
        print(f"Visiting primary link: {self.href1}...")
        for href2 in self.parse(self.href1):
            print(f"Visiting secondary link: {href2}...")
            self.parse(href2)


# create new content column in df
df['content'] = df['url']


# define celery task to reduce scraping bottleneck (i.e. make each QuickScrape instance concurrent)
# this effectively has runtime O(n) where n is the max number of secondary links for any site
@app.task
def scrape(index):
    url = df.iloc[index]['url']
    scraper = QuickScrape(url)
    scraper.execute()
    # save in df
    df.iloc[index]['content'] = scraper.results
    print(f"Scraped {url}!")
    # write df (quick pd method)
    df.to_csv('sites_data/initial.csv', index=False)
    

# output function
def run():
    for index, row in df.iterrows():
        # call delay to activate concurrency
        scrape.delay(index)


# run process when called in python
run()