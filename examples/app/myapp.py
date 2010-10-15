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

@celery.task(accept_magic_kwargs=False)
def add(x, y, **kwargs):
    print("add id: %r %r %r" % (add.request.id, add.request.args,
        add.request.kwargs))
    print("kwargs: %r" % (kwargs, ))
    return x + y

if __name__ == "__main__":
    celery.worker_main()
