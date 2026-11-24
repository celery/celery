.. _broker-kafka:

=============
 Using Kafka
=============

.. _broker-Kafka-installation:

Configuration
=============

For celeryconfig.py:

.. code-block:: python

    import os

    task_serializer = 'json'
    broker_transport_options = {
        # "allow_create_topics": True,
    }
    broker_connection_retry_on_startup = True

    # For using SQLAlchemy as the backend
    # result_backend = 'db+postgresql://postgres:example@localhost/postgres'

    broker_transport_options.update({
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
    })
    sasl_username = os.environ["SASL_USERNAME"]
    sasl_password = os.environ["SASL_PASSWORD"]
    broker_url = f"confluentkafka://{sasl_username}:{sasl_password}@broker:9094"
    broker_transport_options.update({
        "kafka_admin_config": {
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
        },
        "kafka_common_config": {
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "bootstrap.servers": "broker:9094",
        }
    })
    
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

See above. The SASL username and password are passed in as environment variables.

Further Info
============

Celery queues get routed to Kafka topics. For example, if a queue is named "add_queue",
then a topic named "add_queue" will be created/used in Kafka.

For canvas, when using a backend that supports it, the typical mechanisms like
chain, group, and chord seem to work.


Limitations
===========

With classic Kafka consumer groups, each partition is assigned to at most one
worker in the group. The number of workers actively consuming a Celery queue
therefore cannot exceed the number of partitions in its Kafka topic. Workers
without an assigned partition remain idle.

Kombu creates topics with one partition by default. To use multiple workers,
create the topic with at least one partition per worker, or set the
``num_partitions`` transport option when Kombu creates the topic:

.. code-block:: python

    broker_transport_options.update({
        "num_partitions": 4,
    })

The ``num_partitions`` option is only used when Kombu creates a topic. For an
existing topic, increase the number of partitions using Kafka's administration
tools. Note that Kafka only guarantees message ordering within a partition, not
across the whole topic.
