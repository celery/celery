import os
import sys
import time

from celery import Celery

DEFAULT_ITS = 5000

celery = Celery(__name__)
celery.conf.update(BROKER_TRANSPORT="amqp",
                   BROKER_POOL_LIMIT=10,
                   CELERY_PREFETCH_MULTIPLIER=0,
                   CELERY_DISABLE_RATE_LIMITS=True,
                   CELERY_BACKEND=None)


@celery.task(cur=0, time_start=None, queue="bench.worker")
def it(_, n):
    i = it.cur  # use internal counter, as ordering can be skewed
                # by previous runs, or the broker.
    if i and not i % 1000:
        print >> sys.stderr, i
    if not i:
        it.time_start = time.time()
    elif i == n - 1:
        print("consume: %s" % (time.time() - it.time_start, ))
        sys.exit()
    it.cur += 1


def bench_apply(n=DEFAULT_ITS):
    time_start = time.time()
    celery.TaskSet(it.subtask((i, n)) for i in xrange(n)).apply_async()
    print("apply: %s" % (time.time() - time_start, ))


def bench_consume(n=DEFAULT_ITS):
    from celery.worker import WorkController
    from celery.worker import state

    #import logging
    #celery.log.setup_logging_subsystem(loglevel=logging.DEBUG)
    worker = celery.WorkController(pool_cls="solo",
                                   queues=["bench.worker"])

    try:
        print("STARTING WORKER")
        worker.start()
    except SystemExit:
        assert sum(state.total_count.values()) == n + 1


def bench_both(n=DEFAULT_ITS):
    bench_apply(n)
    bench_consumer(n)


def main(argv=sys.argv):
    if len(argv) < 2:
        print("Usage: %s [apply|consume|both]" % (os.path.basename(argv[0]), ))
        return sys.exit(1)
    return {"apply": bench_apply,
            "consume": bench_consume,
            "both": bench_both}[argv[1]]()


if __name__ == "__main__":
    main()
