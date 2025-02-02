from celery import Celery

app = Celery()


@app.task
def plugh():
    """This task is in a different module!"""
