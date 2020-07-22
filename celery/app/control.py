"""Worker Remote Control Client.

Client for worker remote control commands.
Server implementation is in :mod:`celery.worker.control`.
"""
import warnings

from billiard.common import TERM_SIGNAME
from kombu.matcher import match
from kombu.pidbox import Mailbox
from kombu.utils.compat import register_after_fork
from kombu.utils.functional import lazy
from kombu.utils.objects import cached_property

from celery.exceptions import DuplicateNodenameWarning
from celery.utils.log import get_logger
from celery.utils.text import pluralize

__all__ = ('Inspect', 'Control', 'flatten_reply')

logger = get_logger(__name__)

W_DUPNODE = """\
Received multiple replies from node {0}: {1}.
Please make sure you give each node a unique nodename using
the celery worker `-n` option.\
"""


def flatten_reply(reply):
    """Flatten node replies.

    Convert from a list of replies in this format::

        [{'a@example.com': reply},
         {'b@example.com': reply}]

    into this format::

        {'a@example.com': reply,
         'b@example.com': reply}
    """
    nodes, dupes = {}, set()
    for item in reply:
        [dupes.add(name) for name in item if name in nodes]
        nodes.update(item)
    if dupes:
        warnings.warn(DuplicateNodenameWarning(
            W_DUPNODE.format(
                pluralize(len(dupes), 'name'), ', '.join(sorted(dupes)),
            ),
        ))
    return nodes


def _after_fork_cleanup_control(control):
    try:
        control._after_fork()
    except Exception as exc:  # pylint: disable=broad-except
        logger.info('after fork raised exception: %r', exc, exc_info=1)


class Inspect:
    """API for app.control.inspect."""

    app = None

    def __init__(self, destination=None, timeout=1.0, callback=None,
                 connection=None, app=None, limit=None, pattern=None,
                 matcher=None):
        self.app = app or self.app
        self.destination = destination
        self.timeout = timeout
        self.callback = callback
        self.connection = connection
        self.limit = limit
        self.pattern = pattern
        self.matcher = matcher

    def _prepare(self, reply):
        if reply:
            by_node = flatten_reply(reply)
            if (self.destination and
                    not isinstance(self.destination, (list, tuple))):
                return by_node.get(self.destination)
            if self.pattern:
                pattern = self.pattern
                matcher = self.matcher
                return {node: reply for node, reply in by_node.items()
                        if match(node, pattern, matcher)}
            return by_node

    def _request(self, command, **kwargs):
        return self._prepare(self.app.control.broadcast(
            command,
            arguments=kwargs,
            destination=self.destination,
            callback=self.callback,
            connection=self.connection,
            limit=self.limit,
            timeout=self.timeout, reply=True,
            pattern=self.pattern, matcher=self.matcher,
        ))

    def report(self):
        return self._request('report')

    def clock(self):
        return self._request('clock')

    def active(self, safe=None):
        # safe is ignored since 4.0
        # as no objects will need serialization now that we
        # have argsrepr/kwargsrepr.
        return self._request('active')

    def scheduled(self, safe=None):
        return self._request('scheduled')

    def reserved(self, safe=None):
        return self._request('reserved')

    def stats(self):
        return self._request('stats')

    def revoked(self):
        return self._request('revoked')

    def registered(self, *taskinfoitems):
        return self._request('registered', taskinfoitems=taskinfoitems)
    registered_tasks = registered

    def ping(self, destination=None):
        if destination:
            self.destination = destination
        return self._request('ping')

    def active_queues(self):
        return self._request('active_queues')

    def query_task(self, *ids):
        # signature used be unary: query_task(ids=[id1, id2])
        # we need this to preserve backward compatibility.
        if len(ids) == 1 and isinstance(ids[0], (list, tuple)):
            ids = ids[0]
        return self._request('query_task', ids=ids)

    def conf(self, with_defaults=False):
        return self._request('conf', with_defaults=with_defaults)

    def hello(self, from_node, revoked=None):
        return self._request('hello', from_node=from_node, revoked=revoked)

    def memsample(self):
        return self._request('memsample')

    def memdump(self, samples=10):
        return self._request('memdump', samples=samples)

    def objgraph(self, type='Request', n=200, max_depth=10):
        return self._request('objgraph', num=n, max_depth=max_depth, type=type)


class Control:
    """Worker remote control client."""

    Mailbox = Mailbox

    def __init__(self, app=None):
        self.app = app
        self.mailbox = self.Mailbox(
            app.conf.control_exchange,
            type='fanout',
            accept=['json'],
            producer_pool=lazy(lambda: self.app.amqp.producer_pool),
            queue_ttl=app.conf.control_queue_ttl,
            reply_queue_ttl=app.conf.control_queue_ttl,
            queue_expires=app.conf.control_queue_expires,
            reply_queue_expires=app.conf.control_queue_expires,
        )
        register_after_fork(self, _after_fork_cleanup_control)

    def _after_fork(self):
        del self.mailbox.producer_pool

    @cached_property
    def inspect(self):
        return self.app.subclass_with_self(Inspect, reverse='control.inspect')

    def purge(self, connection=None):
        """Discard all waiting tasks.

        This will ignore all tasks waiting for execution, and they will
        be deleted from the messaging server.

        Arguments:
            connection (kombu.Connection): Optional specific connection
                instance to use.  If not provided a connection will
                be acquired from the connection pool.

        Returns:
            int: the number of tasks discarded.
        """
        with self.app.connection_or_acquire(connection) as conn:
            return self.app.amqp.TaskConsumer(conn).purge()
    discard_all = purge

    def election(self, id, topic, action=None, connection=None):
        self.broadcast(
            'election', connection=connection, destination=None,
            arguments={
                'id': id, 'topic': topic, 'action': action,
            },
        )

    def revoke(self, task_id, destination=None, terminate=False,
               signal=TERM_SIGNAME, **kwargs):
        """Tell all (or specific) workers to revoke a task by id (or list of ids).

        If a task is revoked, the workers will ignore the task and
        not execute it after all.

        Arguments:
            task_id (Union(str, list)): Id of the task to revoke
                (or list of ids).
            terminate (bool): Also terminate the process currently working
                on the task (if any).
            signal (str): Name of signal to send to process if terminate.
                Default is TERM.

        See Also:
            :meth:`broadcast` for supported keyword arguments.
        """
        return self.broadcast('revoke', destination=destination, arguments={
            'task_id': task_id,
            'terminate': terminate,
            'signal': signal,
        }, **kwargs)

    def terminate(self, task_id,
                  destination=None, signal=TERM_SIGNAME, **kwargs):
        """Tell all (or specific) workers to terminate a task by id (or list of ids).

        See Also:
            This is just a shortcut to :meth:`revoke` with the terminate
            argument enabled.
        """
        return self.revoke(
            task_id,
            destination=destination, terminate=True, signal=signal, **kwargs)

    def ping(self, destination=None, timeout=1.0, **kwargs):
        """Ping all (or specific) workers.

        Returns:
            List[Dict]: List of ``{'hostname': reply}`` dictionaries.

        See Also:
            :meth:`broadcast` for supported keyword arguments.
        """
        return self.broadcast(
            'ping', reply=True, arguments={}, destination=destination,
            timeout=timeout, **kwargs)

    def rate_limit(self, task_name, rate_limit, destination=None, **kwargs):
        """Tell workers to set a new rate limit for task by type.

        Arguments:
            task_name (str): Name of task to change rate limit for.
            rate_limit (int, str): The rate limit as tasks per second,
                or a rate limit string (`'100/m'`, etc.
                see :attr:`celery.task.base.Task.rate_limit` for
                more information).

        See Also:
            :meth:`broadcast` for supported keyword arguments.
        """
        return self.broadcast(
            'rate_limit',
            destination=destination,
            arguments={
                'task_name': task_name,
                'rate_limit': rate_limit,
            },
            **kwargs)

    def add_consumer(self, queue,
                     exchange=None, exchange_type='direct', routing_key=None,
                     options=None, destination=None, **kwargs):
        """Tell all (or specific) workers to start consuming from a new queue.

        Only the queue name is required as if only the queue is specified
        then the exchange/routing key will be set to the same name (
        like automatic queues do).

        Note:
            This command does not respect the default queue/exchange
            options in the configuration.

        Arguments:
            queue (str): Name of queue to start consuming from.
            exchange (str): Optional name of exchange.
            exchange_type (str): Type of exchange (defaults to 'direct')
                command to, when empty broadcast to all workers.
            routing_key (str): Optional routing key.
            options (Dict): Additional options as supported
                by :meth:`kombu.entity.Queue.from_dict`.

        See Also:
            :meth:`broadcast` for supported keyword arguments.
        """
        return self.broadcast(
            'add_consumer',
            destination=destination,
            arguments=dict({
                'queue': queue,
                'exchange': exchange,
                'exchange_type': exchange_type,
                'routing_key': routing_key,
            }, **options or {}),
            **kwargs
        )

    def cancel_consumer(self, queue, destination=None, **kwargs):
        """Tell all (or specific) workers to stop consuming from ``queue``.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'cancel_consumer', destination=destination,
            arguments={'queue': queue}, **kwargs)

    def time_limit(self, task_name, soft=None, hard=None,
                   destination=None, **kwargs):
        """Tell workers to set time limits for a task by type.

        Arguments:
            task_name (str): Name of task to change time limits for.
            soft (float): New soft time limit (in seconds).
            hard (float): New hard time limit (in seconds).
            **kwargs (Any): arguments passed on to :meth:`broadcast`.
        """
        return self.broadcast(
            'time_limit',
            arguments={
                'task_name': task_name,
                'hard': hard,
                'soft': soft,
            },
            destination=destination,
            **kwargs)

    def enable_events(self, destination=None, **kwargs):
        """Tell all (or specific) workers to enable events.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'enable_events', arguments={}, destination=destination, **kwargs)

    def disable_events(self, destination=None, **kwargs):
        """Tell all (or specific) workers to disable events.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'disable_events', arguments={}, destination=destination, **kwargs)

    def pool_grow(self, n=1, destination=None, **kwargs):
        """Tell all (or specific) workers to grow the pool by ``n``.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'pool_grow', arguments={'n': n}, destination=destination, **kwargs)

    def pool_shrink(self, n=1, destination=None, **kwargs):
        """Tell all (or specific) workers to shrink the pool by ``n``.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'pool_shrink', arguments={'n': n},
            destination=destination, **kwargs)

    def autoscale(self, max, min, destination=None, **kwargs):
        """Change worker(s) autoscale setting.

        See Also:
            Supports the same arguments as :meth:`broadcast`.
        """
        return self.broadcast(
            'autoscale', arguments={'max': max, 'min': min},
            destination=destination, **kwargs)

    def shutdown(self, destination=None, **kwargs):
        """Shutdown worker(s).

        See Also:
            Supports the same arguments as :meth:`broadcast`
        """
        return self.broadcast(
            'shutdown', arguments={}, destination=destination, **kwargs)

    def pool_restart(self, modules=None, reload=False, reloader=None,
                     destination=None, **kwargs):
        """Restart the execution pools of all or specific workers.

        Keyword Arguments:
            modules (Sequence[str]): List of modules to reload.
            reload (bool): Flag to enable module reloading.  Default is False.
            reloader (Any): Function to reload a module.
            destination (Sequence[str]): List of worker names to send this
                command to.

        See Also:
            Supports the same arguments as :meth:`broadcast`
        """
        return self.broadcast(
            'pool_restart',
            arguments={
                'modules': modules,
                'reload': reload,
                'reloader': reloader,
            },
            destination=destination, **kwargs)

    def heartbeat(self, destination=None, **kwargs):
        """Tell worker(s) to send a heartbeat immediately.

        See Also:
            Supports the same arguments as :meth:`broadcast`
        """
        return self.broadcast(
            'heartbeat', arguments={}, destination=destination, **kwargs)

    def broadcast(self, command, arguments=None, destination=None,
                  connection=None, reply=False, timeout=1.0, limit=None,
                  callback=None, channel=None, pattern=None, matcher=None,
                  **extra_kwargs):
        """Broadcast a control command to the celery workers.

        Arguments:
            command (str): Name of command to send.
            arguments (Dict): Keyword arguments for the command.
            destination (List): If set, a list of the hosts to send the
                command to, when empty broadcast to all workers.
            connection (kombu.Connection): Custom broker connection to use,
                if not set, a connection will be acquired from the pool.
            reply (bool): Wait for and return the reply.
            timeout (float): Timeout in seconds to wait for the reply.
            limit (int): Limit number of replies.
            callback (Callable): Callback called immediately for
                each reply received.
            pattern (str): Custom pattern string to match
            matcher (Callable): Custom matcher to run the pattern to match
        """
        with self.app.connection_or_acquire(connection) as conn:
            arguments = dict(arguments or {}, **extra_kwargs)
            if pattern and matcher:
                # tests pass easier without requiring pattern/matcher to
                # always be sent in
                return self.mailbox(conn)._broadcast(
                    command, arguments, destination, reply, timeout,
                    limit, callback, channel=channel,
                    pattern=pattern, matcher=matcher,
                )
            else:
                return self.mailbox(conn)._broadcast(
                    command, arguments, destination, reply, timeout,
                    limit, callback, channel=channel,
                )
