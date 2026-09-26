from kombu import Queue

from celery.utils.nodenames import default_nodename, gethostname, worker_direct


class test_worker_direct:

    def test_returns_if_queue(self):
        q = Queue('foo')
        assert worker_direct(q) is q


class test_default_nodename:

    def test_defaults_to_celery_at_local_host(self):
        assert default_nodename(None) == f'celery@{gethostname()}'

    def test_bare_value_is_the_host(self):
        assert default_nodename('example.com') == 'celery@example.com'

    def test_full_nodename_is_kept(self):
        assert default_nodename('foo@example.com') == 'foo@example.com'

    def test_default_name_replaces_only_a_missing_name(self):
        assert default_nodename('example.com', 'celerybeat') == (
            'celerybeat@example.com')
        assert default_nodename(None, 'celerybeat') == (
            f'celerybeat@{gethostname()}')
        assert default_nodename('foo@example.com', 'celerybeat') == (
            'foo@example.com')
