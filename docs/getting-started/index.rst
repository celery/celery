=================
 Getting Started
=================

:Release: |version|
:Date: |today|

.. toctree::
    :maxdepth: 2

    introduction
    brokers/index
    first-steps-with-celery
    next-steps
    resources

Backends and brokers
====================

*Note: This section is not comprehensive of backends and brokers.*

Celery has the ability to communicate and store with many different backends (Result Stores) and brokers (Message Transports).

Redis
-----

Redis can be both a backend and a broker.

**As a Broker:** Redis works well for rapid transport of small messages. Large messages can congest the system.

`See broker documentation for details`_

.. _`See broker documentation for details`: https://docs.celeryproject.org/en/stable/getting-started/brokers/redis.html

**As a Backend:** Redis is a super fast K/V store, making it very efficient for fetching the results of a task call. As with the design of Redis, you do have to consider the limit memory available to store your data, and how you handle data persistence. If result persistence is important, consider using another DB for your backend.

RabbitMQ
--------

RabbitMQ is a broker.

RabbitMQ handles larger messages better than Redis, however if many messages are coming in very quickly, scaling can become a concern and Redis or SQS should be considered unless RabbitMQ is running at very large scale.

`See documentation for details`_

.. _`See documentation for details`: https://docs.celeryproject.org/en/stable/getting-started/brokers/rabbitmq.html

*Note: RabbitMQ (as the broker) and Redis (as the backend) are very commonly used together. If more guaranteed long-term persistence is needed from the result store, consider using PostgreSQL or MySQL (through SQLAlchemy), Cassandra, or a custom defined backend.*

SQS
---

SQS is a broker.

If you already integrate tightly with AWS, and are familiar with SQS, it presents a great option as a broker. It is extremely scalable and completely managed, and manages task delegation similarly to RabbitMQ. It does lack some of the features of the RabbitMQ broker such as ``worker remote control commands``.

`See documentation for details`_

.. _`See documentation for details`: https://docs.celeryproject.org/en/stable/getting-started/brokers/sqs.html

SQLAlchemy
----------

SQLAlchemy is backend.

It allows Celery to interface with MySQL, PostgreSQL, SQlite, and more. It is a ORM, and is the way Celery can use a SQL DB as a result backend. Historically, `SQLAlchemy has not been the most stable result backend`_ so if chosen one should proceed with caution.

.. _`SQLAlchemy has not been the most stable result backend`: https://docs.celeryproject.org/en/3.1/getting-started/brokers/sqlalchemy.html
