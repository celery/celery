"""
    celery.beat.scheduler.failover.redis_failover
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Redis failover strategy to make Celery Beat high available with broker Redis

    Not Implemented yet.
"""

from celery.beat.scheduler.failover.base_failover import BaseFailoverStrategy


class RedisFailoverStrategy(BaseFailoverStrategy):
    connection_type = "redis"

    def is_master(self):
        return super(RedisFailoverStrategy, self).is_master()

