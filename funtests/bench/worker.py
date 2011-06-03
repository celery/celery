import time

from celery import Celery

celery = Celery()
celery.conf.update(BROKER_TRANSPORT="memory",
                   BROKER_POOL_LIMIT=1,
                   CELERY_PREFETCH_MULTIPLIER=0,
                   CELERY_DISABLE_RATE_LIMITS=True,
                   CELERY_BACKEND=None)


def bench_consumer(n=10000):
    from celery.worker import WorkController
    from celery.worker import state

    worker = WorkController(app=celery, pool_cls="solo")
    time_start = [None]

    @celery.task()
    def it(i):
        if not i:
            time_start[0] = time.time()
        elif i == n - 1:
            print(time.time() - time_start[0])

    @celery.task()
    def shutdown_worker():
        raise SystemExit()

    for i in xrange(n):
        it.delay(i)
    shutdown_worker.delay()

    try:
        worker.start()
    except SystemExit:
        assert sum(state.total_count.values()) == n + 1


if __name__ == "__main__":
    bench_consumer()
