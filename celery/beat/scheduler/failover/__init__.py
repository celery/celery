"""
    celery.beat.scheduler.failover
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


"""

from amqp_failover import AMQPFailoverStrategy
from redis_failover import RedisFailoverStrategy