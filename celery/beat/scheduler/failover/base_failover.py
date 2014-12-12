"""
    celery.beat.scheduler.failover.base_failover
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    An abstraction for beat failover strategies. This provides information about the node is master or not.
"""

from celery.utils.log import get_logger
from celery.five import with_metaclass

logger = get_logger(__name__)

failover_strategy_registry = {}


class BaseFailoverStrategyMeta(type):
    def __new__(cls, *args, **kwargs):
        m = type.__new__(cls, *args, **kwargs)
        if hasattr(m, "connection_type") and m.connection_type:
            failover_strategy_registry[m.connection_type] = m
        return m


@with_metaclass(BaseFailoverStrategyMeta)
class BaseFailoverStrategy(object):
    connection_type = None


    def __init__(self, app, failover_broker_url=None, *args, **kwargs):
        self.app = app
        self.failover_broker_url = failover_broker_url

    def is_master(self):
        """
        Override this method and define your way to detect whether the node is master or not. A lock mechanism can be used.
        :return:boolean
        """
        raise NotImplementedError