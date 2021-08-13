# Neo: a distributed task queue scraper

Neo is a super-fast, super-lightweight scraper capable of handling around 300
requests at once. It's built using the Celery distributed task queue, and configured to run on Windows OS (unsupported by Celery). 

Neo scales using green threads by using Eventlet as Celery's execution pool, which also happens to be the only way for Celery to work on Windows. For large input sizes, manual queue routing is used to achieve multiprocessing (another hack for Windows).

Implemented on an input size of 57k+ as a commercial data project, Neo combines with a regex parser called Trinity to find compatibility of site pages with CRM products and produces a ranking score. Neo's observed latency speedup is 315x during production, reducing average latency from 29s to 9.2ms. For context, this meant a reduction in total runtime from 472 hrs to 1.5 hrs.

Code references The Matrix for bonus points.