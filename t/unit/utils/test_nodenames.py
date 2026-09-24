import pytest

from kombu import Queue

from celery.utils.nodenames import host_format, worker_direct


class test_worker_direct:

    def test_returns_if_queue(self):
        q = Queue('foo')
        assert worker_direct(q) is q


class test_host_format:

    @pytest.mark.parametrize('template,expected', [
        ('logs/%%n-%n.log', 'logs/%n-worker.log'),
        ('logs/%%i.log', 'logs/%i.log'),
        ('logs/%%I.log', 'logs/%I.log'),
    ])
    def test_percent_escape(self, template, expected):
        assert host_format(template, host='worker.example.com') == expected
