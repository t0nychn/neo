"""Test to see that the pipeline logic works with Celery. It will save to sqlite3 then pull to csv
at sites_data/scrape_test.csv.

(Before running first set up the message broker (e.g. Redis) in your .env file.)

Then set up Celery worker to run file:
$ celery -A pipeline_test worker -P eventlet --concurrency 100

--purge flag can be used to discontinue any messages if previous worker interrupted before finishing
"""

from scraper import Pipeline
import pandas as pd
from celery import Celery
from decouple import config


df = pd.read_csv('sites_list/main.csv')

reader = pd.read_csv('sites_list/main.csv', encoding='utf-8', iterator=True)
df = reader.get_chunk(10).dropna()

# initiate celery instance
app = Celery('data_generator', broker=config('BROKER_URL'))

# counter for completion (takes into account for bad requests)
completed = 0

# define celery task
@app.task
def scrape(index):
    row = df.iloc[index]
    data = Pipeline(
        row['url'], 
        id=int(row['id']),
        name=row['name']
        )
    data.save()
    global completed
    completed += 1
    print(f"Completed: {row['url']}! ({completed}/{len(df)})")
    if completed == len(df):
        # save to csv
        data.yeet('sites_data/pipeline_test.csv')
    return


# output function
def run():
    for index in range(len(df)):
        # call delay to activate concurrency
        scrape.delay(index)


# run process when called in python
run()