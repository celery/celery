"""

Sending and Receiving Messages

"""

from celery.app import app_or_default

default_app = app_or_default()
TaskPublisher = default_app.amqp.TaskPublisher
ConsumerSet = default_app.amqp.ConsumerSet
TaskConsumer = default_app.amqp.TaskConsumer


def establish_connection(**kwargs):
    """Establish a connection to the message broker."""
    # FIXME: # Deprecate
    app = app_or_default(kwargs.pop("app", None))
    return app.broker_connection(**kwargs)


def with_connection(fun):
    """Decorator for providing default message broker connection for functions
    supporting the `connection` and `connect_timeout` keyword
    arguments."""
    # FIXME: Deprecate!
    return default_app.with_default_connection(fun)


def get_consumer_set(connection, queues=None, **options):
    """Get the :class:`kombu.messaging.Consumer` for a queue
    configuration.

    Defaults to the queues in :setting:`CELERY_QUEUES`.

    """
    # FIXME: Deprecate!
    return default_app.amqp.get_task_consumer(connection, queues, **options)
