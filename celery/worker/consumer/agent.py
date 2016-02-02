from __future__ import absolute_import, unicode_literals

from celery import bootsteps

from .connection import Connection

__all__ = ['Agent']


class Agent(bootsteps.StartStopStep):

    conditional = True
    requires = (Connection,)

    def __init__(self, c, **kwargs):
        self.agent_cls = self.enabled = c.app.conf.worker_agent

    def create(self, c):
        agent = c.agent = self.instantiate(self.agent_cls, c.connection)
        return agent
