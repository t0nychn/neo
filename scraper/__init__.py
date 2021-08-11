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
        find_hrefs: Finds all links in a given page.
        parse: Parses page and append to results.
        execute: Execute scraper instance.
    """

    # cooldown between pages in secs
    COOLDOWN = 5
    # limit for number of pages
    PAGE_LIMIT = 15
    # rest time in seconds after 429 response
    NO_SPAM = 90

    # initialize scraper with href1 (initial (1st) link)
    def __init__(self, href1):
        self.href1 = str(href1)
        self.results = {'links': [], 
        'scores': [], 
        'discounts': [], 
        'freebies': [], 
        'subscriptions': []
        }
    
    def find_hrefs(self, soup, filter=True):
        """Finds all the links in a site page and returns as a set.
        Update: filter links at find_href rather than parse.
        
        Args:
            soup (str): Site HTML.
            filter (bool, optional): Whether to apply filter to hrefs found. Default is True.
        
        Returns:
            set: Set of all relevant links found.
        """

        if filter:
            hrefs = set()
            for tag in soup.find_all('a', href=True):
                href = tag.get('href')
                if len(href) > 2:
                    # prevent going onto other sites & saving internal id links
                    if (self.href1 not in href and href[0] != '/') or href[0] == '#':
                        continue
                    # filter out irrelevant pages (to increase regex accuracy)
                    elif 'terms' in href or 'conditions' in href or 'download' in href or 'policy' in href or 'map' in href or 'login' in href or 'privacy' in href:
                        continue
                    else:
                        hrefs.add(href)
                else:
                    continue
        else:
            hrefs = set(tag.get('href') for tag in soup.find_all('a', href=True))
        return hrefs

    def parse(self, href):
        """Visits the link given and appends Trinity's findings to results attribute.
        
        Args:
            href (str): Link to visit.

        Returns:
            set: Set of all links in the page if page visited successfully, else empty string.
        """

        # stop if no link loaded
        if len(href) == 0:
            return ''
        else:
            # account for shortened internal links
            if self.href1[-1] == '/' and href[0] == '/':
                href = self.href1 + href[1:]
            elif self.href1[-1] != '/' and href[0] == '/':
                href = self.href1 + href
            print(f"Visiting: {href}")
            # will raise exception if connection times out
            response = requests.get(href, timeout=5)
            status_code = int(response.status_code)
            # don't spam
            if status_code == 429:
                print(f"429 response received for {href}\nFreezing this scrape for {Neo.NO_SPAM/60} minutes...")
                time.sleep(Neo.NO_SPAM)
                return ''
            # account for bad requests
            elif status_code > 400:
                # end method
                raise Exception(f"Unreachable: {href}")
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
                return self.find_hrefs(soup)

    def execute(self):
        """Executes Neo instance to parse any given website 2 links deep & update results attribute."""
        
        counter = 0
        # any exceptions at first parse will be recorded by Celery flower as failed task (useful for calculating how many sites visited)
        for href2 in self.parse(self.href1):
            # if first parse successful then handle exceptions from secondary links
            try:
                counter += 1
                if counter > Neo.PAGE_LIMIT:
                    print(f'Too many pages at {self.href1}')
                    # end method
                    return
                elif counter == 1:
                    self.parse(href2)
                else:
                    # cooldown
                    time.sleep(Neo.COOLDOWN)
                    self.parse(href2)
            # handle exceptions & continue through list
            except Exception as e:
                print(f"Exception encountered for {href2}: {e.args}")
                continue



class Pipeline:
    """Construct to save scraper output to database.
    Initiation establishes a connection to the database location specified as DB_URI in .env file.
    Developed & tested on SQLite3.
    
    Use:
        link = 'https://en.wikipedia.org/wiki/The_Matrix'
        pipe = Pipeline()
        pipe.etl(link, id=1, name='Wikipedia')
        pipe.yeet('saved_data.csv')
    
    Args:
        None.
    
    Attributes:
        engine (:obj: database engine): SQLAlchemy engine.
        main (:obj: database table): SQLAlchemy table object. Primary key is link of individual pages.

    Methods:
        etl: Executes scraper and saves results to database.
        yeet: Queries database and exports query results to csv.
    """

    def __init__(self):
        self.engine = create_engine(config('DB_URI'))
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
        # generates table (will not override existing)
        metadata.create_all(self.engine)

    def __clean(self, results, targets={'scores'}):
        """Private method to clean result dict of duplicate values, keeping first of each
        value instance. Returns a set of index values to iterating over results for unique entries.
        Value target defaults to 'scores' (assume same score has same data usefulness), and can be altered.

        Args:
            results (dict): Dictionary of results.
            targets (sequence, optional): Target columns to clean duplicates. Default is 'scores'.

        Returns:
            set: Set of index values for unique rows identified in result dictionary.
        """

        indexes = set()
        for target in targets:
            old = results[target]
            # remove duplicates using set
            new = set(old)
            # use set difference to optimize this loop to achieve linear method runtime
            for score in new.difference(indexes):
                indexes.add(old.index(score))
        return indexes
        
    def etl(self, link, id=0, name='website', scraper=Neo):
        """Extracts information from web link using Neo-like scraper and saves to database
        Note that scraper must be polymorphic/inherit from Neo.
        
        Args:
            link (str): Link to website. 
            id (int, optional): ID for website (not primary key in database since multiple pages share same ID). Default is 0.
            name (str, optional): Name of website. Default is 'website'.
            scraper (:obj: module, optional): Scraper module to load. Default is Neo.
        
        Returns:
            None, saves scraped data to database file.
        """
        # check params before initiating
        if isinstance(link, str) and isinstance(id, int) and isinstance(name, str):
            pass
        else:
            raise ValueError("Please specify args in correct format")
        # quick check to speed up runtime by not scraping duplicates
        if self.engine.execute(f"SELECT COUNT(*) FROM main WHERE id = {id}").fetchall()[0][0] > 0:
            print(f"Already saved & moving on: {link}")
            # end method
            return
        else:
            scrape = scraper(link)
            scrape.execute()
        # filter to see if we have results
        r_len = len(scrape.results['links'])
        if r_len > 0:
            for i in self.__clean(self.results):
                # try inserting into db except if integrity error occurs
                try:
                    href = scrape.results['links'][i]
                    stmt = insert(self.main).values(
                        id=id, 
                        name=name, 
                        link=href,
                        score=scrape.results['scores'][i],
                        discounts=str(scrape.results['discounts'][i]),
                        freebies=str(scrape.results['freebies'][i]),
                        subscriptions=str(scrape.results['subscriptions'][i])
                        )
                    self.engine.execute(stmt)
                    print(f"Successfully saved: {href}")
                except exc.IntegrityError as e:
                    print(f"Encountered SQL integrity error for {stmt}: {e.args}")
                    continue
        else:
            return

    def yeet(self, filepath, query='SELECT * FROM main;', index=False):
        """Extracts items from database using pandas read_sql and exports to csv file.
        
        Args:
            filepath (str): Path to save file.
            query (str, optional): SQL query. Default is 'SELECT * FROM main;'.
            index (bool, optional): Whether to keep DataFrame index. Default is False.

        Returns:
            None, outputs csv file in filepath location.
        """

        print(f"Yeeting selected data to: {filepath}")
        df = pd.read_sql(query, con=self.engine)
        df.to_csv(filepath, mode='wb', encoding='utf-8-sig', index=index)
        print("Kobe!")