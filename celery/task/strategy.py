from carrot.connection import DjangoBrokerConnection

from celery.utils import chunks


def even_time_distribution(task, size, time_window, iterable, **apply_kwargs):
    """With an iterator yielding task args, kwargs tuples, evenly distribute
    the processing of its tasks throughout the time window available.

    :param task: The kind of task (a :class:`celery.task.base.Task`.)
    :param size: Total number of elements the iterator gives.
    :param time_window: Total time available, in minutes.
    :param iterable: Iterable yielding task args, kwargs tuples.
    :param \*\*apply_kwargs: Additional keyword arguments to be passed on to
        :func:`celery.execute.apply_async`.

    Example

        >>> class RefreshAllFeeds(Task):
        ...
        ...     def run(self, **kwargs):
        ...         feeds = Feed.objects.all()
        ...         total = feeds.count()
        ...
        ...         time_window = REFRESH_FEEDS_EVERY_INTERVAL_MINUTES
        ...
        ...         def iter_feed_task_args(iterable):
        ...             for feed in iterable:
        ...                 yield ([feed.feed_url], {}) # args, kwargs tuple
        ...
        ...         it = iter_feed_task_args(feeds.iterator())
        ...
        ...         even_time_distribution(RefreshFeedTask, total,
        ...                                time_window, it)

    """

    bucketsize = size / time_window
    buckets = chunks(iterable, int(bucketsize))

    connection = DjangoBrokerConnection()
    try:
        for bucket_count, bucket in enumerate(buckets):
            # Skew the countdown for items in this bucket by one.
            seconds_eta = (60 * bucket_count if bucket_count else None)

            for args, kwargs in bucket:
                task.apply_async(args=args, kwargs=kwargs,
                                 connection=connection,
                                 countdown=seconds_eta,
                                 **apply_kwargs)
    finally:
        connection.close()
