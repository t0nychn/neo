# Neo: supersonic scraping ⚡

Neo is a super-fast, super-lightweight scraper capable of handling around 300
requests at once. It's managed using the Celery distributed task queue framework, and configured to run on Windows OS (unsupported by Celery). 

Neo scales using green threads by using Eventlet as Celery's execution pool, which also happens to be the only way for Celery to work on Windows. Unlike with the default pre-fork execution pool that spawns child processes, Eventlet threads are managed within a single worker process, so manual queue routing can be used to split the task size between a few workers to achieve multiprocessing.

Built for an input size of 57k+ as a commercial data project, Neo combines with a regex parser called Trinity to find compatibility of site pages with CRM products and produces a ranking score for the first 16 pages of input websites. Neo's observed latency speedup is 315x during production on a quad core machine, reducing average latency from 29s to 9.2ms. For context, this meant a reduction in production runtime from 472h to 1.5h.

Code references The Matrix for bonus points.

![supersonic](https://user-images.githubusercontent.com/79203609/129426913-80145b66-d813-4de5-bc17-75858231d9fc.gif)

## Use 🏃
First set up the message broker and databse URI in your .env file. The production.py file is ready to use (edit the input file as need be). More documentation can be found in script.

To spin up worker processes for the production app:
```
$ celery -A production worker -P eventlet -c 10000 -n worker1 -Q worker1
$ celery -A production worker -P eventlet -c 10000 -n worker2 -Q worker2
$ celery -A production worker -P eventlet -c 10000 -n worker3 -Q worker3
$ celery -A production worker -P eventlet -c 10000 -n worker4 -Q worker4
```
Note that concurrency set using flag -c is more of a limit than a guideline. Runtime concurrency depends on OS and other processes.

Run using:
```
$ python run_production.py
```
