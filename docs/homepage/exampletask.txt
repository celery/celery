from celery.task import task


@task
def add(x, y):
    return x + y
