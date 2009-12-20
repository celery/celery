==============================================
 Worker Revoked Tasks - celery.worker.revoke
==============================================

.. data:: revoked

    A :class:`celery.datastructures.LimitedSet` containing revoked task ids.

    Items expire after one hour, and the structure can only hold
    10000 expired items at a time (about 300kb).
