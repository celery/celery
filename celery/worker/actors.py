from __future__ import absolute_import

from celery.bootsteps import StartStopStep
from celery.utils.imports import instantiate, qualname
from celery.utils.log import get_logger
from celery.worker.consumer import Connection
from cell import Actor
from kombu.common import ignore_errors
from kombu.utils import uuid

logger = get_logger(__name__)
debug, warn, error = logger.debug, logger.warn, logger.error


class Bootstep(StartStopStep):
    requires = (Connection, )

    def __init__(self, c, **kwargs):
        c.agent = None

    def create(self, c):
        agent = c.app.conf.CELERYD_AGENT
        agent = c.agent = self.instantiate(c.app.conf.CELERYD_AGENT,
            connection=c.connection, app=c.app,
        )
        return agent


class ActorProxy(object):
    """A class that represents an actor started remotely."""

    def __init__(self, local_actor, actor_id, async_start_result):
        self.__subject = local_actor.__copy__()
        self.__subject.id = actor_id
        self.async_start_result = async_start_result

    def __getattr__(self, name):
            return getattr(self.__subject, name)

    def wait_to_start(self):
        self.async_start_result._result


class Agent(Actor):
    types = ('round-robin', 'scatter')

    class state(object):

        def _start_actor_consumer(self, actor):
            actor.consumer = actor.Consumer(self.connection.channel())
            actor.consumer.consume()
            self.actor.registry[actor.id] = actor

        def add_actor(self, name, id=None):
            """Add actor to the registry and start the actor's main method."""
            try:
                actor = instantiate(name, connection=self.connection, id=id)
                if actor.id in self.actor.registry:
                    warn('Actor id %r already exists', actor.id)
                self._start_actor_consumer(actor)
                debug('Actor registered: %s', name)
                return actor.id
            except Exception as exc:
                error('Cannot start actor: %r', exc, exc_info=True)

        def stop_all(self):
            self.actor.shutdown()

        def reset(self):
            debug('Resetting active actors')
            for actor in self.actor.registry.itervalues():
                if actor.consumer:
                    ignore_errors(self.connection, actor.consumer.cancel)
                actor.connection = self.connection
                self._start_actor_consumer(actor)

        def stop_actor(self, id):
            try:
                actor = self.actor.registry.pop(id)
            except KeyError:
                pass
            else:
                if actor.consumer and actor.consumer.channel:
                    ignore_errors(self.connection, consumer.cancel)

    def __init__(self, connection, app=None, *args, **kwargs):
        self.connection = connection
        self.app = app
        self.registry = {}
        super(ActorManager, self).__init__(*args, **kwargs)

    def contribute_to_state(self, state):
        state.connection = self.connection
        conninfo = self.app.connection()
        state.connection_errors = conninfo.connection_errors
        state.channel_errors = conninfo.channel_errors
        state.reset()
        return super(ActorsManager, self).contribute_to_state(state)

    def add_actor(self, actor, nowait=False):
        name = qualname(actor)
        actor_id = uuid()
        res = self.call('add_actor', {'name': name, 'id': actor_id},
                        type='round-robin', nowait=True)
        actor_proxy = ActorProxy(actor, actor_id, res)
        return actor_proxy

    def stop_actor_by_id(self, actor_id, nowait=False):
        return self.scatter('stop_actor', {'actor_id': actor_id},
                            nowait=nowait)

    def start(self):
        debug('Starting Agent')

    def _shutdown(self, cancel=True, close=True, clear=True):
        try:
            for actor in self.registry.itervalues():
                if actor and actor.consumer:
                    if cancel:
                        ignore_errors(self.connection, actor.consumer.cancel)
                    if close and actor.consumer.channel:
                        ignore_errors(self.connection,
                                      actor.consumer.channel.close)
        finally:
            if clear:
                self.registry.clear()

    def stop(self):
        self._shutdown(clear=False)

    def shutdown(self):
        self._shutdown(cancel=False)
