"""myapp.py

Usage:

   (window1)$ python myapp.py worker -l info

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32


You can also specify the app to use with celeryd::

    $ celery worker -l info --app=myapp

"""
from celery import Celery

celery = Celery('myapp', broker='amqp://guest@localhost//')


@celery.task()
def add(x, y):
    return x + y

if __name__ == '__main__':
    celery.start()
