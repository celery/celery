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
    result_serializer = 'json'

For tasks.py:

.. code-block:: python

    from celery import Celery

    app = Celery('tasks')
    app.config_from_object('celeryconfig')


    @app.task
    def add(x, y):
        return x + y
