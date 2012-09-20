
import bootsteps
from cell import Actor
from celery.utils.imports import instantiate
from kombu.utils import uuid
from .bootsteps import StartStopComponent
from celery.utils.log import get_logger
from celery.worker.consumer import debug

logger = get_logger(__name__)
info, warn, error, crit = (logger.info, logger.warn,
                           logger.error, logger.critical)


class WorkerComponent(StartStopComponent):
    """This component starts an ActorManager instance if actors support is enabled."""
    name = 'worker.actors-manager'
    consumer = None
    
    def ActorsManager(self, w):
        return (w.actors_manager_cls or ActorsManager)
    
    def include_if(self, w):
        #return w.actors_enabled
        return True
    
    def init(self, w, **kwargs):
        w.actors_manager = None
    
    def create(self, w):
        debug('create ActorsManager')
        actor = w.actors_manager = self.instantiate(self.ActorsManager(w), 
                                                    app = w.app)
        actor.app = w.app
        w.on_consumer_ready_callbacks.append(actor.on_consumer_ready)
        return actor


class ActorProxy(object):
    """
    A class that represents an actor started remotely
    """
    def __init__(self, local_actor, actor_id, async_start_result):
        self.__subject = local_actor.__copy__()
        self.__subject.id = actor_id
        self.async_start_result = async_start_result
    
    def __getattr__(self, name):
            return getattr(self.__subject, name)
    
    def wait_to_start(self):
        self.async_start_result._result
         

class ActorsManager(Actor):
    connection = None
    types = ('round-robin', 'scatter')
    actor_registry = {}
    actors_consumer = None
    connection = None
    app = None
    
    def __init__(self, app=None, *args, **kwargs):
        self.app = app
        super(ActorsManager, self).__init__(*args, **kwargs)
            
    def contribute_to_state(self, state):
        state.actor_registry = self.actor_registry
        state.connection = self.connection
        conninfo = self.app.connection()
        state.connection_errors = conninfo.connection_errors
        state.channel_errors = conninfo.channel_errors
        state.reset()
        return Actor.contribute_to_state(self, state)
    
    class state(Actor.state):
        def _start_actor_consumer(self, actor):
            consumer = actor.Consumer(self.connection.channel())                
            consumer.consume()
            self.actor_registry[actor.id] = (actor, consumer)
            
        def add_actor(self, name, id = None):
            """Add actor to the actor registry and start the actor's main method"""
            try:
                actor = instantiate(name, connection = self.connection, 
                                    id = id)
                self._start_actor_consumer(actor)
                if actor.id in self.actor_registry:
                    warn('Actor with the same id already exists')
                debug('Register actor in the actor registry: %s' % name)
                return actor.id
            except Exception as exc:
                error('Start actor error: %r', exc, exc_info=True)
             
        def stop_all(self):
            for _, (_, consumer) in self.actor_registry.items():
                self.maybe_conn_error(consumer.cancel)
            self.actor_registry.clear()

        def reset(self):
            debug('Resetting active actors')
            print self.actor_registry.items()
            for id, (actor, consumer) in self.actor_registry.items():
                self.maybe_conn_error(consumer.cancel)
                # TODO:setting the connection here seems wrong ?
                actor.connection = self.connection
                self._start_actor_consumer(actor)
    
        def stop_actor(self, actor_id):
            if actor_id in self.actor_registry:
                (_, consumer) = self.actor_registry.pop(actor_id)
                self.maybe_conn_error(consumer.cancel)
                
        def maybe_conn_error(self, fun):
            """Applies function but ignores any connection or channel
            errors raised."""
            try:
                fun()
            except (AttributeError, ) + \
                    self.connection_errors + \
                    self.channel_errors:
                pass
        
    def add_actor(self, actor, nowait=False):
        name = "%s.%s"%(actor.__class__.__module__, 
                        actor.__class__.__name__)
        actor_id = uuid()
        res = self.call('add_actor', {'name': name, 'id' : actor_id}, 
                        type = 'round-robin', nowait = 'True')
        actor_proxy = ActorProxy(actor, actor_id, res)
        return actor_proxy
        
    def stop_actor_by_id(self, actor_id, nowait=False):              
        return self.scatter('stop_actor', {'actor_id' : actor_id}, 
                            nowait=nowait)   
    
    def start(self):
        debug('Starting ActorsManager')
    
    def stop(self):
        if self.actors_consumer:
            self.actors_consumer.cancel()
    
    def on_start(self, connection):
        self.connection = connection
        actor_consumer = self.Consumer(self.connection.channel())
        debug('ActorsManager start consuming blabla')
        self.actor_consumer = actor_consumer
        self.actor_consumer.consume()
        self.contribute_to_state(self.state)
        
    def on_consumer_ready(self, consumer):
        debug('ActorsManager in On consumer ready')
        if consumer.connection: 
            raise Exception('Consumer is ready.')
        consumer.on_reset_connection.append(self.on_start)
        consumer.on_close_connection.append(self.stop)
     