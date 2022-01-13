from celery.concurrency.asyncio import TaskPool


def test_pool():
    x = TaskPool(limit=8)
    x.start()
    x.on_apply()
    x.stop()
