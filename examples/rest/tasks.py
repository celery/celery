import celery

@celery.task
def add(x, y):
    return x + y

@celery.task()
def tsum(numbers):
    return sum(numbers)
