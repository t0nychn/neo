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

    # cooldown between pages in secs
    COOLDOWN = 5
    # limit for number of pages
    PAGE_LIMIT = 15
    # rest time in seconds after 429 response
    NO_SPAM = 60

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

        hrefs = set(soup.find_all('a', href=True))
        return hrefs

    def parse(self, href):
        """Visits the link given and appends Trinity's findings to results attribute.
        
        Args:
            href (str): Link to visit.

        Returns:
            set: Set of all links in the page.
        """

        # stop if no link loaded
        if len(href) == 0:
            return ''
        # account for bad links and prevent scraper following external links
        elif (self.href1 not in href and href[0] != '/') or len(href) < 2:
            print(f"Bad/external link: {href}")
            return ''
        else:
            # account for shortened links
            if self.href1[-1] == '/' and href[0] == '/':
                href = self.href1 + href[1:]
            elif self.href1[-1] != '/' and href[0] == '/':
                href = self.href1 + href
            print(f"Visiting: {href}")
            try:
                response = requests.get(href, timeout=5)
            except Exception:
                print(f"Timed out & moving on: {href}")
                return ''
            # don't spam
            if int(response.status_code) == 429:
                print(f"429 response received for {self.href1}\nFreezing this scrape for {Neo.NO_SPAM/60} minutes...")
                time.sleep(Neo.NO_SPAM)
                return ''
            # account for bad requests
            elif int(response.status_code) > 400:
                print(f"Bad request for {href}: {response.status_code}")
                # end method
                return ''
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
        try:
            for href2 in self.parse(self.href1):
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
        # handle exception to prevent interruptions to pipeline
        except Exception as e:
            print(f"Exception encountered for {self.href1}: {e.args}")
            return


class Pipeline(Neo):
    """Construct to save output from Neo to database url in .env file (developed on sqlite3).
    Uses lazy iterating where Neo only executed when save()/execute() is called.
    
    Use:
        link = 'https://en.wikipedia.org/wiki/The_Matrix'
        data = Pipeline(link, id=1, name='Wikipedia')
        data.save()
    
    Args:
        href1 (str): Initial (1st) link.
        id (int): Internal ID (not primary key).
        name (str): Name of website.
    
    Attributes:
        href1 (str): Initial (1st) link.
        results (dict): Dictionary of results for each page, able to be turned into DataFrame.
        main (:obj: 'Table'): SQLAlchemy table object. Primary key is link of individual pages.

    Methods:
        find_href: Finds all links in a given page.
        parse: Parses page and append to results.
        execute: Execute scraper instance.
        __drop: Drops duplicate scores in each website to shorten data.
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
        
    def save(self):
        # quick check to speed up runtime by not scraping duplicates
        if self.engine.execute(f"SELECT COUNT(*) FROM main WHERE id = {self.id}").fetchall()[0][0] > 0:
            print(f"Already saved & moving on: {self.href1}")
            # end method
            return
        else:
            self.execute()
        # filter to see if we have results
        r_len = len(self.results['links'])
        if r_len > 0:
            for i in self.__clean(self.results):
                # try inserting into db except if error occurs
                try:
                    link=self.results['links'][i]
                    stmt = insert(self.main).values(
                        id=self.id, 
                        name=self.name, 
                        link=link,
                        score=self.results['scores'][i],
                        discounts=str(self.results['discounts'][i]),
                        freebies=str(self.results['freebies'][i]),
                        subscriptions=str(self.results['subscriptions'][i])
                        )
                    self.engine.execute(stmt)
                    print(f"Successfully saved: {link}")
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

        df = pd.read_sql(query, con=self.engine)
        df.to_csv(filepath, mode='wb', encoding='utf-8-sig', index=index)