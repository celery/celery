from __future__ import print_function, unicode_literals

import os
import sys
import time

os.environ.update(
    NOSETPS='yes',
    USE_FAST_LOCALS='yes',
)

from celery import Celery, group
from celery.five import range

DEFAULT_ITS = 40000

BROKER_TRANSPORT = os.environ.get('BROKER', 'librabbitmq')
if hasattr(sys, 'pypy_version_info'):
    BROKER_TRANSPORT = 'pyamqp'

app = Celery('bench_worker')
app.conf.update(
    BROKER_TRANSPORT=BROKER_TRANSPORT,
    BROKER_POOL_LIMIT=10,
    CELERYD_POOL='solo',
    CELERYD_PREFETCH_MULTIPLIER=0,
    CELERY_DISABLE_RATE_LIMITS=True,
    CELERY_DEFAULT_DELIVERY_MODE=1,
    CELERY_QUEUES={
        'bench.worker': {
            'exchange': 'bench.worker',
            'routing_key': 'bench.worker',
            'no_ack': True,
            'exchange_durable': False,
            'queue_durable': False,
            'auto_delete': True,
        }
    },
    CELERY_TASK_SERIALIZER='json',
    CELERY_DEFAULT_QUEUE='bench.worker',
    CELERY_BACKEND=None,
),


def tdiff(then):
    return time.time() - then


@app.task(cur=0, time_start=None, queue='bench.worker', bare=True)
def it(_, n):
    i = it.cur  # use internal counter, as ordering can be skewed
                # by previous runs, or the broker.
    if i and not i % 5000:
        print('({0} so far: {1}s)'.format(i, tdiff(it.subt)), file=sys.stderr)
        it.subt = time.time()
    if not i:
        it.subt = it.time_start = time.time()
    elif i == n - 1:
        total = tdiff(it.time_start)
        print('({0} so far: {1}s)'.format(i, tdiff(it.subt)), file=sys.stderr)
        print('-- process {0} tasks: {1}s total, {2} tasks/s} '.format(
            n, total, n / (total + .0),
        ))
        sys.exit()
    it.cur += 1


def bench_apply(n=DEFAULT_ITS):
    time_start = time.time()
    group(it.s(i, n) for i in range(n))()
    print('-- apply {0} tasks: {1}s'.format(n, time.time() - time_start))


def bench_work(n=DEFAULT_ITS, loglevel='CRITICAL'):
    loglevel = os.environ.get('BENCH_LOGLEVEL') or loglevel
    if loglevel:
        app.log.setup_logging_subsystem(loglevel=loglevel)
    worker = app.WorkController(concurrency=15,
                                queues=['bench.worker'])

    try:
        print('STARTING WORKER')
        worker.start()
    except SystemExit:
        raise
        assert sum(worker.state.total_count.values()) == n + 1


def bench_both(n=DEFAULT_ITS):
    bench_apply(n)
    bench_work(n)


def main(argv=sys.argv):
    n = DEFAULT_ITS
    if len(argv) < 2:
        print('Usage: {0} [apply|work|both] [n=20k]'.format(
            os.path.basename(argv[0]),
        ))
        return sys.exit(1)
    try:
        try:
            n = int(argv[2])
        except IndexError:
            pass
        return {'apply': bench_apply,
                'work': bench_work,
                'both': bench_both}[argv[1]](n=n)
    except:
        raise


if __name__ == '__main__':
    main()
