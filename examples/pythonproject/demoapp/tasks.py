from celery.task import task


@task()
def add(x, y):
    return x + y


@task()
def mul(x, y):
    return x * y
