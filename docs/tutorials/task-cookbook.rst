.. _cookbook-tasks:

================
 Task Cookbook
================

.. contents::
    :local:

.. _cookbook-task-serial:

Ensuring a task is only executed one at a time
==============================================

You can accomplish this by using a lock.

In this example we'll be using the cache framework to set a lock that's
accessible for all workers.

It's part of an imaginary RSS feed importer called `djangofeeds`.
The task takes a feed URL as a single argument, and imports that feed into
a Django model called `Feed`. We ensure that it's not possible for two or
more workers to import the same feed at the same time by setting a cache key
consisting of the MD5 check-sum of the feed URL.

The cache key expires after some time in case something unexpected happens,
and something always will...

For this reason your tasks run-time shouldn't exceed the timeout.


.. note::

    In order for this to work correctly you need to be using a cache
    backend where the ``.add`` operation is atomic.  ``memcached`` is known
    to work well for this purpose.

.. code-block:: python

    from celery import task
    from celery.five import monotonic
    from celery.utils.log import get_task_logger
    from contextlib import contextmanager
    from django.core.cache import cache
    from hashlib import md5
    from djangofeeds.models import Feed

    logger = get_task_logger(__name__)

    LOCK_EXPIRE = 60 * 10  # Lock expires in 10 minutes

    @contextmanager
    def memcache_lock(lock_id, oid):
        timeout_at = monotonic() + LOCK_EXPIRE - 3
        # cache.add fails if the key already exists
        status = cache.add(lock_id, oid, LOCK_EXPIRE)
        try:
            yield status
        finally:
            # memcache delete is very slow, but we have to use it to take
            # advantage of using add() for atomic locking
            if monotonic() < timeout_at:
                # don't release the lock if we exceeded the timeout
                # to lessen the chance of releasing an expired lock
                # owned by someone else.
                cache.delete(lock_id)

    @task(bind=True)
    def import_feed(self, feed_url):
        # The cache key consists of the task name and the MD5 digest
        # of the feed URL.
        feed_url_hexdigest = md5(feed_url).hexdigest()
        lock_id = '{0}-lock-{1}'.format(self.name, feed_url_hexdigest)
        logger.debug('Importing feed: %s', feed_url)
        with memcache_lock(lock_id, self.app.oid) as acquired:
            if acquired:
                return Feed.objects.import_feed(feed_url).url
        logger.debug(
            'Feed %s is already being imported by another worker', feed_url)
