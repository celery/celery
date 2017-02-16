"""Celery + :pypi:`cell` integration."""
from typing import Any
from celery import bootsteps
from celery.types import WorkerConsumerT
from .connection import Connection

__all__ = ['Agent']


class Agent(bootsteps.StartStopStep):
    """Agent starts :pypi:`cell` actors."""

    conditional = True
    requires = (Connection,)

    def __init__(self, c: WorkerConsumerT, **kwargs) -> None:
        self.agent_cls = self.enabled = c.app.conf.worker_agent
        super(Agent, self).__init__(c, **kwargs)

    def create(self, c: WorkerConsumerT) -> Any:
        agent = c.agent = self.instantiate(self.agent_cls, c.connection)
        return agent
