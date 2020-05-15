from __future__ import absolute_import, print_function, unicode_literals

import os
import sys

from kombu.five import monotonic  # noqa

from celery import Celery  # noqa
from celery.five import range  # noqa

os.environ.update(
    NOSETPS='yes',
    USE_FAST_LOCALS='yes',
)


DEFAULT_ITS = 40000

BROKER_TRANSPORT = os.environ.get('BROKER', 'librabbitmq://')
if hasattr(sys, 'pypy_version_info'):
    BROKER_TRANSPORT = 'pyamqp://'

app = Celery('bench_worker')
app.conf.update(
    broker_url=BROKER_TRANSPORT,
    broker_pool_limit=10,
    worker_pool='solo',
    worker_prefetch_multiplier=0,
    task_default_delivery_mode=1,
    task_queues={
        'bench.worker': {
            'exchange': 'bench.worker',
            'routing_key': 'bench.worker',
            'no_ack': True,
            'exchange_durable': False,
            'queue_durable': False,
            'auto_delete': True,
        }
    },
    task_serializer='json',
    task_default_queue='bench.worker',
    result_backend=None,
),


def tdiff(then):
    return monotonic() - then


@app.task(cur=0, time_start=None, queue='bench.worker', bare=True)
def it(_, n):
    # use internal counter, as ordering can be skewed
    # by previous runs, or the broker.
    i = it.cur
    if i and not i % 5000:
        print('({0} so far: {1}s)'.format(i, tdiff(it.subt)), file=sys.stderr)
        it.subt = monotonic()
    if not i:
        it.subt = it.time_start = monotonic()
    elif i > n - 2:
        total = tdiff(it.time_start)
        print('({0} so far: {1}s)'.format(i, tdiff(it.subt)), file=sys.stderr)
        print('-- process {0} tasks: {1}s total, {2} tasks/s'.format(
            n, total, n / (total + .0),
        ))
        import os
        os._exit()
    it.cur += 1


def bench_apply(n=DEFAULT_ITS):
    time_start = monotonic()
    task = it._get_current_object()
    with app.producer_or_acquire() as producer:
        [task.apply_async((i, n), producer=producer) for i in range(n)]
    print('-- apply {0} tasks: {1}s'.format(n, monotonic() - time_start))


def bench_work(n=DEFAULT_ITS, loglevel='CRITICAL'):
    loglevel = os.environ.get('BENCH_LOGLEVEL') or loglevel
    if loglevel:
        app.log.setup_logging_subsystem(loglevel=loglevel)
    worker = app.WorkController(concurrency=15,
                                queues=['bench.worker'])

    try:
        print('-- starting worker')
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
