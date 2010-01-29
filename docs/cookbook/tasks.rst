================
 Creating Tasks
================


Ensuring a task is only executed one at a time
----------------------------------------------

You can accomplish this by using a lock.

In this example we'll be using the cache framework to set a lock that is
accessible for all workers.

It's part of an imaginary RSS feed importer called ``djangofeeds``.
The task takes a feed URL as a single argument, and imports that feed into
a Django model called ``Feed``. We ensure that it's not possible for two or
more workers to import the same feed at the same time by setting a cache key
consisting of the md5sum of the feed URL.

The cache key expires after some time in case something unexpected happens
(you never know, right?)

.. code-block:: python

    from celery.task import Task
    from django.core.cache import cache
    from django.utils.hashcompat import md5_constructor as md5
    from djangofeeds.models import Feed

    LOCK_EXPIRE = 60 * 5 # Lock expires in 5 minutes

    class FeedImporter(Task):
        name = "feed.import"

        def run(self, feed_url, **kwargs):
            logger = self.get_logger(**kwargs)

            # The cache key consists of the task name and the MD5 digest
            # of the feed URL.
            feed_url_digest = md5(feed_url).hexdigest()
            lock_id = "%s-lock-%s" % (self.name, feed_url_hexdigest)

            is_locked = lambda: str(cache.get(lock_id)) == "true"
            acquire_lock = lambda: cache.set(lock_id, "true", LOCK_EXPIRE)
            # memcache delete is very slow, so we'd rather set a false value
            # with a very low expiry time.
            release_lock = lambda: cache.set(lock_id, "nil", 1)

            logger.debug("Importing feed: %s" % feed_url)
            if is_locked():
                logger.debug(
                    "Feed %s is already being imported by another worker" % (
                        feed_url))
                return

            acquire_lock()
            try:
                feed = Feed.objects.import_feed(feed_url)
            finally:
                release_lock()

            return feed.url
