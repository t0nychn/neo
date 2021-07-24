"""
This is a simple exercise to demonstrate the power of using Celery
to run functions concurrently. We will use the Eventlet library to
scale Celery's concurrent processes far beyond the number of processors
on machine. Eventlet uses greenlets to implement lightweight thread-like
structures that are scheduled and managed inside the process.

(Before running first set up the message broker (e.g. Redis) in your .env file.)

First emulate a single-threaded process:
$ celery -A scale_test worker -P eventlet --concurrency 1

Then:
$ celery -A scale_test worker -P eventlet --concurrency 50

Finally:
$ celery -A scale_test worker -P eventlet --concurrency 100

Note that multi-(green)threading may not produce faster results if we
do not require the add() function to sleep. But for runtime bottlenecks
caused by non-CPU intensive tasks, such as waiting for a web response,
an Eventlet pool is a real treat.
"""

from celery import Celery
from decouple import config
import time

app = Celery('test', broker=config('BROKER_URL'))

counter = 0

@app.task
def add(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter
    counter += 1
    print(f"Complete: {counter}/100")
    
for i in range(100):
    add.delay(i, i)
    