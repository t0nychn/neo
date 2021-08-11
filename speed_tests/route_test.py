"""This is to test whether multiple Celery workers can be run on a Windows machine,
to enable the scraper to spawn parallel workers and split green threads for each worker.

Set up a celery flower to view worker processes in browser interface using 
$ celery -A route_test flower

Example test via seperate terminals:
$ celery -A route_test worker -P eventlet -c 2500 -n worker1 -Q worker1
$ celery -A route_test worker -P eventlet -c 1000 -n worker2 -Q worker2
$ python start_route_test.py

-note we can also give a worker multiple queues

Results (on task size of 20,000, runtime including Celery worker setup time):
    test = 2x 10,000 & concurrency = 2x 1,000 -> 100 secs
    test = 2x 10,000 & concurrency = 2x 10,000 -> 64 secs (6x base, average concurrency reached: 2x 1,400)
    test = 3x 6,666 & concurrency = 3x 6,666 -> 54 secs (5x base, average concurrency reached: 3x 1,200)
    test = 4x 5,000 & concurrency = 4x 5,000 -> 52 secs (5x base, average concurrency reached: 4x 900)
    test = 8x 2,500 & concurrency = 8x 2,500 -> 70 secs (7x base, average concurrency reached: 8x 600 - also temporarily killed 4 workers)

The results indicate that for a task size of 20,000 where each task takes 10 secs to run,
we can manually route Celery workers to 2 different task queues each with 
task size of 10,000 and concurrency of 10,000 to have a runtime of around 58% that of a single worker.
The optimal number of workers is 4, at 49% the runtime of a single worker.

While not quite as parallel as hoped, probably due to the CPU intensive nature of addition,
using multiple workers can still achieve around 50% more parallelism.
"""

from celery import Celery
from decouple import config
import time

app = Celery('route_test', broker=config('BROKER_URL'))
counter1 = 0
counter2 = 0
counter3 = 0
counter4 = 0
counter5 = 0
counter6 = 0
counter7 = 0
counter8 = 0

test = 1000

@app.task
def add1(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter1
    counter1 += 1
    print(f"Complete: {counter1}/{test}")

@app.task
def add2(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter2
    counter2 += 1
    print(f"Complete: {counter2}/{test}")

@app.task
def add3(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter3
    counter3 += 1
    print(f"Complete: {counter3}/{test}")

@app.task
def add4(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter4
    counter4 += 1
    print(f"Complete: {counter4}/{test}")

@app.task
def add5(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter5
    counter5 += 1
    print(f"Complete: {counter5}/{test}")

@app.task
def add6(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter6
    counter6 += 1
    print(f"Complete: {counter6}/{test}")

@app.task
def add7(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter7
    counter7 += 1
    print(f"Complete: {counter7}/{test}")

@app.task
def add8(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter8
    counter8 += 1
    print(f"Complete: {counter8}/{test}")

def run():
    for i in range(test):
        # delete/add as appropriate
        add1.apply_async(args=[i, i], queue='worker1')
        add2.apply_async(args=[i, i], queue='worker2')