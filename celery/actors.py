from __future__ import absolute_import

from celery.app import app_or_default

import cl
import cl.presence


def construct(cls, instance, connection=None, *args, **kwargs):
    app = instance.app = app_or_default(kwargs.pop("app", None))
    super(cls, instance).__init__(connection or app.broker_connection(),
                                  *args, **kwargs)


class Actor(cl.Actor):

    def __init__(self, *args, **kwargs):
        construct(Actor, self, *args, **kwargs)


class Agent(cl.Agent):

    def __init__(self, *args, **kwargs):
        construct(Agent, self, *args, **kwargs)


class AwareAgent(cl.presence.AwareAgent):

    def __init__(self, *args, **kwargs):
        construct(AwareAgent, self, *args, **kwargs)
