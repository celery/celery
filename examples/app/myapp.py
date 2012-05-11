"""myapp.py

Usage:

   (window1)$ python myapp.py worker -l info

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32


You can also specify the app to use with celeryd::

    $ celeryd -l info --app=myapp.celery

"""
from celery import Celery


def debug_args(fun):
    from kombu.utils import reprcall

    def _inner(self, *args, **kwargs):
        print("CALL: %r" % reprcall(self.name, args, kwargs))
        return fun(*args, **kwargs)
    return _inner



celery = Celery("myapp")
celery.conf.update(
    BROKER_URL="amqp://guest:guest@localhost:5672//",
    CELERY_ANNOTATIONS={
        "myapp.add": {"@__call__": debug_args},
    },
)


@celery.task
def add(x, y):
    return x + y

if __name__ == "__main__":
    celery.start()
