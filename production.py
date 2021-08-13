"""Final production file of the scraper for input size of around 48,000. 
It will save to sqlite3 then pull to csv at sites_data/final.csv.

(Before running first set up the message broker (e.g. Redis) in your .env file.)

Set up Celery flower to view running tasks:
$ celery -A production flower

Then set up Celery workers:
$ celery -A production worker -P eventlet -c 12000 -n worker1 -Q worker1
$ celery -A production worker -P eventlet -c 12000 -n worker2 -Q worker2
$ celery -A production worker -P eventlet -c 12000 -n worker3 -Q worker3
$ celery -A production worker -P eventlet -c 12000 -n worker4 -Q worker4

Check all workers are online before finally:
$ python run_production.py
"""

from scraper import Pipeline
import pandas as pd
from celery import Celery
from decouple import config
import csv
import math


# define input file target (for debug function use sites_list/debug.csv)
file = 'sites_list/main.csv'

# initiate celery instance
app = Celery('production', broker=config('BROKER_URL'))

# initiate pipeline
pipe = Pipeline()

# quick line counting
with open(file, encoding='utf-8') as f:
    line_count = len(list(csv.reader(f)))
split = math.ceil(line_count/4)
print(f"Task size: {line_count}\nSplit: 4x {split}")

# save as iterable chunks
chunks = pd.read_csv(file, encoding='utf-8', chunksize=split)

# iterate chunks out to memory
chunk1 = next(chunks)
chunk2 = next(chunks)
chunk3 = next(chunks)
chunk4 = next(chunks)


# debug function
@app.task
def debug(index):
    print(index)


# main functions (cannot pass df directly into args since not JSON encodable)
@app.task
def scrape1(index):
    row = chunk1.loc[index]
    pipe.etl(
        row['url'], 
        id=int(row['id']),
        name=row['name']
        )

@app.task
def scrape2(index):
    row = chunk2.loc[index]
    pipe.etl(
        row['url'], 
        id=int(row['id']),
        name=row['name']
        )

@app.task
def scrape3(index):
    row = chunk3.loc[index]
    pipe.etl(
        row['url'], 
        id=int(row['id']),
        name=row['name']
        )

@app.task
def scrape4(index):
    row = chunk4.loc[index]
    pipe.etl(
        row['url'], 
        id=int(row['id']),
        name=row['name']
        )


# output function
def run():
    for index in chunk1.index:
        scrape1.apply_async(args=[index], queue='worker1')
    for index in chunk2.index:
        scrape2.apply_async(args=[index], queue='worker2')
    for index in chunk3.index:
        scrape3.apply_async(args=[index], queue='worker3')
    for index in chunk4.index:
        scrape4.apply_async(args=[index], queue='worker4')