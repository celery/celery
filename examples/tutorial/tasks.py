from __future__ import absolute_import

from celery import Celery

celery = Celery('tasks', broker='amqp://')

@celery.task()
def add(x, y):
    return x + y

if __name__ == '__main__':
    celery.start()
