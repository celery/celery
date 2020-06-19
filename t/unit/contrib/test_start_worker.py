from celery.contrib.testing.worker import start_worker
from celery.utils.nodenames import anon_nodename


class test_StartWorker:
    def test_start_worker_hostname(self):
        with start_worker(self.app, hostname='foo') as worker:
            assert worker.hostname == 'celery@foo'
    
    def test_start_worker_hostname_unspecified(self):
        with start_worker(self.app) as worker:
            assert worker.hostname == anon_nodename()
