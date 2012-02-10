import os
import sys
import time

os.environ["NOSETPS"] = "yes"

import anyjson
JSONIMP = os.environ.get("JSONIMP")
if JSONIMP:
    anyjson.force_implementation(JSONIMP)

print("anyjson implementation: %r" % (anyjson.implementation.name, ))

from celery import Celery

DEFAULT_ITS = 20000

BROKER_TRANSPORT = "librabbitmq"
if hasattr(sys, "pypy_version_info"):
    BROKER_TRANSPORT = "amqplib"

celery = Celery(__name__)
celery.conf.update(BROKER_TRANSPORT=BROKER_TRANSPORT,
                   BROKER_POOL_LIMIT=10,
                   CELERYD_POOL="solo",
                   CELERY_PREFETCH_MULTIPLIER=0,
                   CELERY_DISABLE_RATE_LIMITS=True,
                   CELERY_DEFAULT_DELIVERY_MODE=1,
                   CELERY_QUEUES = {
                       "bench.worker": {
                           "exchange": "bench.worker",
                           "routing_key": "bench.worker",
                           "no_ack": True,
                           "exchange_durable": False,
                           "queue_durable": False,
                           "auto_delete": True,
                        }
                   },
                   CELERY_TASK_SERIALIZER="json",
                   CELERY_DEFAULT_QUEUE="bench.worker",
                   CELERY_BACKEND=None,
                   )#CELERY_MESSAGE_COMPRESSION="zlib")


def tdiff(then):
    return time.time() - then


@celery.task(cur=0, time_start=None, queue="bench.worker")
def it(_, n):
    i = it.cur  # use internal counter, as ordering can be skewed
                # by previous runs, or the broker.
    if i and not i % 5000:
        print >> sys.stderr, "(%s so far: %ss)" % (i, tdiff(it.subt))
        it.subt = time.time()
    if not i:
        it.subt = it.time_start = time.time()
    elif i == n - 1:
        total = tdiff(it.time_start)
        print >> sys.stderr, "(%s so far: %ss)" % (i, tdiff(it.subt))
        print("-- process %s tasks: %ss total, %s tasks/s} " % (
                n, total, n / (total + .0)))
        sys.exit()
    it.cur += 1


def bench_apply(n=DEFAULT_ITS):
    time_start = time.time()
    celery.TaskSet(it.subtask((i, n)) for i in xrange(n)).apply_async()
    print("-- apply %s tasks: %ss" % (n, time.time() - time_start, ))


def bench_work(n=DEFAULT_ITS, loglevel="CRITICAL"):
    loglevel = os.environ.get("BENCH_LOGLEVEL") or loglevel
    if loglevel:
        celery.log.setup_logging_subsystem(loglevel=loglevel)
    worker = celery.WorkController(concurrency=15,
                                   queues=["bench.worker"])

    try:
        print("STARTING WORKER")
        worker.start()
    except SystemExit:
        assert sum(worker.state.total_count.values()) == n + 1


def bench_both(n=DEFAULT_ITS):
    bench_apply(n)
    bench_work(n)


def main(argv=sys.argv):
    n = DEFAULT_ITS
    if len(argv) < 2:
        print("Usage: %s [apply|work|both] [n=20k]" % (
                os.path.basename(argv[0]), ))
        return sys.exit(1)
    try:
        try:
            n = int(argv[2])
        except IndexError:
            pass
        return {"apply": bench_apply,
                "work": bench_work,
                "both": bench_both}[argv[1]](n=n)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
