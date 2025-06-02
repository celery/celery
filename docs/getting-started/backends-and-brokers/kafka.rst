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
            "bootstrap_servers": "broker:9094",
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

Currently, using Kafka as a broker means that only one worker can be used.
See https://github.com/celery/kombu/issues/1785.
