from celery import Celery

app = Celery(
    'myapp',
    broker='redis://',
    backend='redis://',
)
