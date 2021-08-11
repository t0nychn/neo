"""
This is a simple exercise to demonstrate the power of using Celery
to run functions concurrently. We will use the Eventlet library to
scale Celery's concurrent processes far beyond the number of processors
on machine. Eventlet uses greenlets to implement lightweight thread-like
structures that are scheduled and managed inside the Celery worker as
child processes.

(Before running first set up the message broker (e.g. Redis) in your .env file.)

Simple example using test = 100:
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

By running a few tests, we can see how cocurrency compares to parallel.
If we assume a theoretical base time of 10 seconds (delay in add function),
we can get an estimate for how Eventlet's concurrent process scales as a factor of
the base runtime.

Results (runtime including Celery worker setup time):
    test = 100 & concurrency = 10 -> 100 secs (10x 1x base)
    test = 100 & concurrency = 100 -> 10 secs ((1x) 1x base)
    test = 1,000 & concurrency = 1,000 -> 10 secs (1x base)
    test = 2,000 & concurrency = 2,000 -> 17 secs (2x base)
    test = 5,000 & concurrency = 5,000 -> 30 secs (3x base)
    test = 20,000 & concurrency = 20,000 -> 107 secs (11x base)
    test = 50,000 & concurrency = 50,000 -> 3 mins 56 secs (24x base)

    Average actual concurrency reached during testing is around 2,500

Hence, for a test batch of 100 that takes around 2 minutes to complete at 100 concurrency, 
we can expect it to take at least 48 minutes if the input size and concurrency are scaled 
up to 50,000. Contrastingly, we expect true parallelism to maintain the 1x base speed
no matter the input size. Hence, we see that using green threads fail to achieve parallelism
at large input sizes. Such is perhaps why the default concurrency in Eventlet is set to 1,000.

Nevertheless, an Eventlet pool managed by Celery is still far more scalable than implementing
a truly parallel multiprocessing program, which is restricted to the number of processors on
machine (usually 8). A smart solution may be to run multiple workers in parallel, with each worker
running its own green threads. In such a way, scaling from 100 to 50,000 inputs will only result 
in roughly 3x increase in runtime (50,000/8 = 6,250).

Update 2021-08-11: unable to spawn multiple on Windows machine using celery's multi start command :/
Will try manual routing to different queues and starting workers in terminal (see routing_test).
"""

from celery import Celery
from decouple import config
import time

app = Celery('scale_test', broker=config('BROKER_URL'))

counter = 0

test = 100

@app.task
def add(x, y):
    print("Sleeping for 10 seconds")
    time.sleep(10)
    print(f"{x} + {y} = {str(x+y)}")
    global counter
    counter += 1
    print(f"Complete: {counter}/{test}")
    
for i in range(test):
    add.delay(i, i)