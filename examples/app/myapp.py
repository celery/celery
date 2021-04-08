"""myapp.py

Usage::

   (window1)$ python myapp.py worker -l INFO

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32


You can also specify the app to use with the `celery` command,
using the `-A` / `--app` option::

    $ celery -A myapp worker -l INFO

With the `-A myproj` argument the program will search for an app
instance in the module ``myproj``.  You can also specify an explicit
name using the fully qualified form::

    $ celery -A myapp:app worker -l INFO

"""
from random import randint
from time import sleep

from celery import Celery

app = Celery(
    'myapp',
    broker='amqp://guest@localhost//',
    # ## add result backend here if needed.
    backend='redis://'
)

app.conf.task_acks_late = True


@app.task
def add(x, y):
    return x + y


@app.task
def s():
    sleep(randint(30, 120))


if __name__ == '__main__':
    app.start()
