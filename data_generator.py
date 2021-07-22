"""
This file is for pulling textual data from a sample of 100 websites from csv/input.csv
to have enough data with which to write regex tests. Output will be saved to sites_data/initial.csv

Set up redis server:
$redis-server

Set up celery worker to run file:
$celery -A data_generator worker -P eventlet --concurrency 1000

--purge flag can be used to discontinue any messages if previous worker interrupted before finishing
"""

import pandas as pd
from celery import Celery
from bs4 import BeautifulSoup
from decouple import config
import requests
import time


# load data & drop rows with missing urls
reader = pd.read_csv('sites_list/main.csv', encoding='utf-8', iterator=True)
df = reader.get_chunk(100).dropna()


# initiate celery instance
app = Celery('data_generator', broker=config('BROKER_URL'))


# define scraper
class QuickScrape:
    """Quick scraper that goes 2 links deep into one site and collects all text in result attribute"""
    # limit for number of pages
    PAGE_LIMIT = 15

    # initialize scraper with href1 (initial (1st) link)
    def __init__(self, href1):
        self.href1 = str(href1)
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
        if (self.href1 not in href and href[0] != '/') or len(href) < 2:
            print(f"Bad/external link: {href}")
        else:
            # account for shortened links
            if self.href1[-1] == '/' and href[0] == '/':
                href = self.href1 + href[1:]
            elif self.href1[-1] != '/' and href[0] == '/':
                href = self.href1 + href
            print(f"Visiting: {href}...")
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
        counter = 0
        for href2 in self.parse(self.href1):
            # account for links back to home
            if self.href1 == href2:
                print(f"Duplicate request: {href2}")
            counter += 1
            if counter > QuickScrape.PAGE_LIMIT:
                print(f'Too many pages at {self.href1}!')
                # end function
                return
            else:
                # cooldown
                time.sleep(2)
                self.parse(href2)


# create new df of scraped values from scratch
id = []
name = []
url = []
content = []

# define celery task to reduce scraping bottleneck (i.e. make each QuickScrape instance concurrent)
# this effectively has runtime O(n) where n is the max number of secondary links for any site
@app.task
def scrape(index):
    href1 = df.iloc[index]['url']
    scraper = QuickScrape(href1)
    scraper.execute()
    # save in df if managed to get something
    if scraper.results:
        id.append(df.iloc[index]['id'])
        name.append(df.iloc[index]['name'])
        url.append(href1)
        content.append(scraper.results)
    print(f"Scraped {href1}! ({len(content)}/{len(df)})")
    # this will slightly slow runtime but 80-20 approach for now
    # necessary to prevent celery from writing to file too early
    new_df = pd.DataFrame({'id':id, 'name':name, 'url':url, 'content':content})
    # save to csv
    new_df.to_csv('sites_data/initial.csv', encoding='utf-8-sig', index=False)
    

# output function
def run():
    for index in range(len(df)):
        # call delay to activate concurrency
        scrape.delay(index)


# run process when called in python
run()