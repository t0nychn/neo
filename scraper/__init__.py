"""This file contains the scraper module Neo adapted from QuickScrape in data_generator.py."""

import requests
from bs4 import BeautifulSoup
import time
from regex import Trinity
from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, insert, exc
from decouple import config
import pandas as pd

class Neo:
    """Scraper that goes 2 links deep into a given site and applies the Trinity parser
    to return links, scores, discounts, freebies and subscriptions as dictionary

    Use:
        link = 'https://en.wikipedia.org/wiki/The_Matrix'
        scraper = Neo(link)
        scraper.execute()
        results = scraper.results
    
    Args:
        href1 (str): Initial (1st) link.
    
    Attributes:
        href1 (str): Initial (1st) link.
        results (dict): Dictionary of results for each page, able to be turned into DataFrame.

    Methods:
        find_href: Finds all links in a given page.
        parse: Parses page and append to results.
        execute: Execute scraper instance.
    """

    # limit for number of pages
    PAGE_LIMIT = 15
    # rest time in seconds after 429 response
    NO_SPAM = 120

    # initialize scraper with href1 (initial (1st) link)
    def __init__(self, href1):
        self.href1 = str(href1)
        self.results = {'links': [], 
        'scores': [], 
        'discounts': [], 
        'freebies': [], 
        'subscriptions': []
        }
    
    def find_href(self, soup):
        """Finds all the links in a site page and returns as a set.
        
        Args:
            soup (str): Site HTML.
        
        Returns:
            set: Set of all links found.
        """

        hrefs = set()
        for tag in soup.find_all('a', href=True):
            hrefs.add(tag.get('href'))
        return hrefs

    def parse(self, href):
        """Visits the link given and appends Trinity's findings to results attribute.
        
        Args:
            href (str): Link to visit.

        Returns:
            set: Set of all links in the page.
        """

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
            # don't spam
            if int(response.status_code) == 429:
                print(f"429 response received for {href}\nRetrying in {Neo.NO_SPAM/60} minutes...")
                time.sleep(Neo.NO_SPAM)
            # account for bad requests
            elif int(response.status_code) > 400:
                print(f"Bad request for {href}: {response.status_code}")
            else:
                soup = BeautifulSoup(response.text, 'html.parser')
                # call Trinity to parse page and append to results attribute
                trin = Trinity(soup.get_text(strip=True))
                score = trin.score()
                if score > 0:
                    self.results['links'].append(href)
                    self.results['scores'].append(score)
                    self.results['discounts'].append(trin.disc)
                    self.results['freebies'].append(trin.free)
                    self.results['subscriptions'].append(trin.subs)
                else:
                    pass
                # return list of links
                return self.find_href(soup)


    def execute(self):
        """Executes Neo instance to parse any given website 2 links deep."""
        
        counter = 0
        for href2 in self.parse(self.href1):
            # account for links back to home
            if self.href1 == href2:
                print(f"Duplicate request: {href2}")
            counter += 1
            if counter > Neo.PAGE_LIMIT:
                print(f'Too many pages at {self.href1}!')
                # end function
                return
            else:
                # cooldown
                time.sleep(5)
                self.parse(href2)


class Pipeline(Neo):
    """Construct to save output from Neo to database url in .env file (developed on sqlite3).
    
    Use:
        link = 'https://en.wikipedia.org/wiki/The_Matrix'
        data = Pipeline(link, id=1, name='Wikipedia')
        data.commit()
    
    Args:
        href1 (str): Initial (1st) link.
        id (int): Internal ID (not primary key).
        name (str): Name of website.
    
    Attributes:
        href1 (str): Initial (1st) link.
        results (dict): Dictionary of results for each page, able to be turned into DataFrame.
        main (:obj: 'Table'): SQLAlchemy table object.

    Methods:
        find_href: Finds all links in a given page.
        parse: Parses page and append to results.
        execute: Execute scraper instance.
        save: Commit scraped data to database.
        yeet: Extracts database values into csv.
    """

    def __init__(self, href1, id=None, name=None):
        # check params before initiating
        if isinstance(href1, str) and isinstance(id, int) and isinstance(name, str):
            pass
        else:
            raise ValueError("Please specify args in correct format")
        Neo.__init__(self, href1)
        self.engine = create_engine(config('DB_URI'))
        self.id = id
        self.name = name
        metadata = MetaData()

        # declare table
        self.main = Table('main', metadata,
            Column('id', Integer),
            Column('name', String),
            Column('link', String, primary_key=True),
            Column('score', Integer),
            Column('discounts', String),
            Column('freebies', String),
            Column('subscriptions', String))
        # generate table (will not override existing)
        metadata.create_all(self.engine)

    def save(self):
        # quick (but imperfect) check to speed up runtime by not scraping duplicates
        if self.engine.execute(f"SELECT COUNT(*) FROM main WHERE link = '{self.href1}'").fetchall()[0][0] > 0:
            print(f"Already saved & moving on: {self.href1}")
            # end method
            return
        else:
            self.execute()
        results = self.results
        # filter to see if we have results
        r_len = len(results['links'])
        if r_len > 0:
            for i in range(r_len):
                # try inserting into db except if primary key constraint violated - i.e. link exists
                try:
                    link=results['links'][i]
                    stmt = insert(self.main).values(
                        id=self.id, 
                        name=self.name, 
                        link=link,
                        score=results['scores'][i],
                        discounts=str(results['discounts'][i]),
                        freebies=str(results['freebies'][i]),
                        subscriptions=str(results['subscriptions'][i])
                        )
                    self.engine.execute(stmt)
                    print(f"Successfully saved: {link}!")
                except exc.IntegrityError as e:
                    print(f"Handled SQL Integrity Error: {e}")
                    continue
        else:
            return

    def yeet(self, filepath, query='SELECT * FROM main;', index=False):
        """Extracts items from databse using pandas and writes to csv file.
        
        Args:
            filepath (str): Path to save file.
            query (str, optional): SQL query. Default is 'SELECT * FROM main;'.
            index (bool, optional): Whether to keep DataFrame index. Default is False.

        Returns:
            None, outputs csv file in filepath location.
        """

        df = pd.read_sql(query, con=self.engine)
        df.to_csv(filepath, mode='wb', encoding='utf-8-sig', index=index)