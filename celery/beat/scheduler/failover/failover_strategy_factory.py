"""
    celery.beat.scheduler.failover.failover_strategy_factory
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Factory to generate a strategy by broker url or custom celery beat failover broker url.
    It decides by the broker backend part of the url.
"""
from celery.beat.scheduler.failover.base_failover import failover_strategy_registry


class FailoverStrategyFactory(object):
    def __init__(self, app):
        self.app = app
        self.strategies = {}

    def get_failover_strategy(self, *args, **kwargs):
        """ CELERYBEAT_FAILOVER_BROKER_URL is prior."""
        failover_broker_url = self.app.conf.first("CELERYBEAT_FAILOVER_BROKER_URL", "BROKER_URL")
        parts = failover_broker_url.split(":")
        if len(parts) > 0:
            connection_type = parts[0]
            strategy_key = "%s.%s" % (self.app.main, connection_type)
            if strategy_key in self.strategies:
                return self.strategies[strategy_key]
            else:

                strategy_cls = failover_strategy_registry.get(connection_type, None)
                if strategy_cls and callable(strategy_cls):
                    strategy = strategy_cls(self.app, failover_broker_url, *args, **kwargs)
                    self.strategies[strategy_key] = strategy
                    return strategy



