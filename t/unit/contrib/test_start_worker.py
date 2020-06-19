from celery.contrib.testing.worker import start_worker


class test_StartWorker:
    def test_start_worker_hostname(self):
        with start_worker(hostname='foo') as worker:
            self.assertEqual(worker.hostname, 'foo')
