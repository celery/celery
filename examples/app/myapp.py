"""myapp.py

Usage:

   (window1)$ python myapp.py -l info

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32

"""
from celery import Celery


celery = Celery("myapp")
celery.conf.update(BROKER_HOST="localhost")

@celery.task()
def add(x, y):
    return x + y

if __name__ == "__main__":
    celery.worker_main()
