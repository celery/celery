.. _broker-kafka:

=============
 Using Kafka
=============

.. _broker-Kafka-installation:

Configuration
=============

For celeryconfig.py:

.. code-block:: python

    broker_url = 'confluentkafka://localhost:9092'
    result_backend = 'redis://localhost:6379/0'
    broker_transport_options = {"allow_create_topics": True}
    task_serializer = 'json'

Please note that "allow_create_topics" is needed if the topic does not exist
yet but is not necessary otherwise.

For tasks.py:

.. code-block:: python

    from celery import Celery

    app = Celery('tasks')
    app.config_from_object('celeryconfig')


    @app.task
    def add(x, y):
        return x + y

Auth
====

TODO: Document how to properly authenticate with a kafka broker that uses SASL, for example.

Further Info
============

Celery queues get routed to Kafka topics. For example, if a queue is named "add_queue",
then a topic named "add_queue" will be created/used in Kafka.

For canvas, when using a backend that supports it, the typical mechanisms like
chain, group, and chord seem to work.
