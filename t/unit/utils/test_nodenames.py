from kombu import Queue

from celery.utils.nodenames import host_format, worker_direct


class test_worker_direct:

    def test_returns_if_queue(self):
        q = Queue('foo')
        assert worker_direct(q) is q


def test_host_format_percent_escape():
    assert host_format('logs/%%n-%n.log', host='worker.example.com') == 'logs/%n-worker.log'
