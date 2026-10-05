from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from pytest_celery import RabbitMQTestBroker

from celery import Celery, beat, uuid


@pytest.mark.filterwarnings(
    'error::pytest.PytestUnhandledThreadExceptionWarning',
)
class test_beat_shutdown:
    @pytest.mark.parametrize(
        'scheduler_cls', [beat.Scheduler, beat.PersistentScheduler],
    )
    def test_threaded_shutdown_closes_broker_resources(
        self,
        celery_rabbitmq_broker: RabbitMQTestBroker,
        tmp_path: Path,
        scheduler_cls: type[beat.Scheduler],
    ):
        """Stopped Beat instances must release their publishing sockets."""
        queue_name = f'beat-shutdown-{uuid()}'
        retained = []
        with Celery(
            'beat-shutdown',
            broker=celery_rabbitmq_broker.container.host_url,
            set_as_current=False,
        ) as app:
            app.conf.result_expires = None
            with app.connection_for_read() as reader:
                with reader.SimpleQueue(queue_name, no_ack=True) as queue:
                    try:
                        for cycle in range(2):
                            app.conf.beat_schedule = {
                                'shutdown-probe': {
                                    'task': 'beat_shutdown_probe',
                                    'args': (cycle,),
                                    'schedule': timedelta(days=1),
                                    'options': {
                                        'queue': queue_name,
                                        'ignore_result': True,
                                    },
                                    'last_run_at': (
                                        app.now() - timedelta(days=2)
                                    ),
                                },
                            }
                            embedded = beat.EmbeddedService(
                                app,
                                thread=True,
                                remote_control=False,
                                scheduler_cls=scheduler_cls,
                                schedule_filename=str(
                                    tmp_path / f'schedule-{cycle}'
                                ),
                            )
                            connection = None
                            embedded.start()
                            try:
                                try:
                                    message = queue.get(timeout=15)
                                    assert message.headers['task'] == (
                                        'beat_shutdown_probe'
                                    )
                                    assert message.payload[0] == [cycle]
                                    # Receiving a task confirms Beat opened
                                    # its shelf and cached its resources.
                                    scheduler = embedded.service.scheduler
                                    connection = scheduler.connection
                                    channel = scheduler.producer.channel
                                    sock = connection.connection.sock
                                    assert connection.connected
                                    assert channel.is_open
                                    assert sock.fileno() >= 0
                                finally:
                                    # Bound the wait so a broken shutdown
                                    # cannot hang the test indefinitely.
                                    embedded.service.stop(wait=False)
                                    embedded.join(timeout=5)
                                    assert not embedded.is_alive(), (
                                        'Beat did not stop'
                                    )

                                retained.append(
                                    (embedded, connection, channel, sock)
                                )
                                assert not connection.connected
                                assert not channel.is_open
                                assert sock.fileno() == -1
                            finally:
                                # Cleanup after assertions, including on the
                                # unfixed code where the scheduler retains it.
                                if connection is None:
                                    scheduler = embedded.service.__dict__.get(
                                        'scheduler'
                                    )
                                    if scheduler is not None:
                                        connection = scheduler.__dict__.get(
                                            'connection'
                                        )
                                if connection is not None:
                                    connection.release()

                        # Keep references to every stopped instance and its
                        # resources so garbage collection cannot hide the leak.
                        assert len(retained) == 2
                        assert all(
                            not connection.connected and not channel.is_open
                            and sock.fileno() == -1
                            for _, connection, channel, sock in retained
                        )
                    finally:
                        # Cancel the consumer before deleting its queue so
                        # RabbitMQ does not send a server-side Basic.Cancel.
                        queue.close()
                        queue.queue.delete()
