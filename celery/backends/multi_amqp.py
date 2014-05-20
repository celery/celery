import socket
from collections import defaultdict

from celery import states
from celery.five import monotonic
from celery.backends.amqp import AMQPBackend
from celery.utils.log import get_logger


logger = get_logger(__name__)


class MultiAMQPBackend(AMQPBackend):
    '''
    Just like AMQPBackend, but allows to receive multiple task
    results, e.g. from fanout exchange.
    '''

    #: This backend supports multiple results
    supports_multi = True

    def get_task_metas(self, task_id, backlog_limit=1000,
                       expected_replies=None):
        '''
        Just like AMQPBackend, but do keep history.
        '''
        with self.app.pool.acquire_channel(block=True) as (_, channel):
            binding = self._create_binding(task_id)(channel)
            binding.declare()

            replies = []
            for i in range(expected_replies or backlog_limit):
                reply = binding.get(accept=self.accept, no_ack=True)

                if not reply:  # no more messages
                    break
                if reply.payload['task_id'] == task_id:
                    replies.append(reply)
                # Fast requeue
                reply.requeue()
            else:
                if not expected_replies:
                    raise self.BacklogLimitExceeded(task_id)

            if replies:
                try:
                    self._cache[task_id] += [r.payload for r in replies]
                except KeyError:
                    self._cache[task_id] = {r.payload['hostname']: r.payload
                                            for r in replies}
                return self._cache[task_id]
            else:
                # no new states, use cached
                try:
                    return self._cache[task_id]
                except KeyError:
                    # result probably pending
                    return [{'status': states.PENDING, 'result': None}]

    def store_result(self, task_id, result, status,
                     traceback=None, request=None, **kwargs):
        '''
        Send task return value and status.
        '''
        routing_key, correlation_id = self.destination_for(task_id, request)
        if not routing_key:
            return
        meta = {'task_id': task_id,
                'status': status,
                'result': self.encode_result(result, status),
                'traceback': traceback,
                'children': self.current_task_children(request),
                'hostname': request.hostname}
        with self.app.amqp.producer_pool.acquire(block=True) as producer:
            producer.publish(
                meta,
                exchange=self.exchange,
                routing_key=routing_key,
                correlation_id=correlation_id,
                serializer=self.serializer,
                retry=True,
                retry_policy=self.retry_policy,
                declare=self.on_reply_declare(task_id),
                delivery_mode=self.delivery_mode,
            )
        return result

    def wait_for(self, task_id, expected_replies=None,
                 timeout=None, cache=True,
                 no_ack=True, on_interval=None,
                 READY_STATES=states.READY_STATES,
                 PROPAGATE_STATES=states.PROPAGATE_STATES,
                 **kwargs):
        '''
        Wait for result of task.
        '''
        cached_metas = self._cache.get(task_id)
        if cache and cached_metas:
            for meta in cached_metas:
                if meta['status'] in READY_STATES:
                    yield meta
        # If there are results to consume, get them
        if not(expected_replies is not None and expected_replies == 0):
            for meta in self.consume(task_id, expected_replies,
                                     timeout=timeout, no_ack=no_ack,
                                     on_interval=on_interval):
                yield meta

    def drain_events(self, connection, consumer, task_id,
                     expected_replies=None, timeout=None, on_interval=None,
                     now=monotonic, wait=None):
        wait = wait or connection.drain_events
        results = defaultdict(list)

        current_result = []

        def callback(meta, message):
            if meta['status'] in states.READY_STATES:
                results[meta['task_id']].append(meta)
                current_result.append(meta)
                self._cache.update(results)

        consumer.callbacks[:] = [callback]
        time_start = now()

        while True:
            if expected_replies:
                # Everybody responded, stop the cycle
                if len(results[task_id]) == expected_replies:
                    break
            # Total time spent may exceed a single call to wait()
            if timeout and now() - time_start >= timeout:
                # Don't raise socket.timeout exception here, because
                # we still want to collect results from already
                # replied workers
                logger.info('collecting results for task {0} timed out'.format(
                            task_id))
                break
            try:
                wait(timeout=1)  # timeout)
            except socket.timeout:
                pass
            if on_interval:
                on_interval()
            if current_result:
                yield current_result.pop()

    def consume(self, task_id, expected_replies=None,
                timeout=None, no_ack=True, on_interval=None):
        wait = self.drain_events
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            binding = self._create_binding(task_id)
            with self.Consumer(channel, binding,
                               no_ack=no_ack, accept=self.accept) as consumer:
                for res in wait(conn, consumer, task_id, expected_replies,
                                timeout, on_interval):
                    yield res
