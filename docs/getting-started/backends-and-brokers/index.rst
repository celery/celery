.. _brokers:

======================
 Backends and Brokers
======================

:Release: |version|
:Date: |today|

Celery supports several message transport alternatives.

.. _broker_toc:

Broker Instructions
===================

.. toctree::
    :maxdepth: 1

    rabbitmq
    redis
    sqs

.. _broker-overview:

Broker Overview
===============

This is comparison table of the different transports supports,
more information can be found in the documentation for each
individual transport (see :ref:`broker_toc`).

+---------------+--------------+----------------+--------------------+
| **Name**      | **Status**   | **Monitoring** | **Remote Control** |
+---------------+--------------+----------------+--------------------+
| *RabbitMQ*    | Stable       | Yes            | Yes                |
+---------------+--------------+----------------+--------------------+
| *Redis*       | Stable       | Yes            | Yes                |
+---------------+--------------+----------------+--------------------+
| *Amazon SQS*  | Stable       | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Zookeeper*   | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+

Experimental brokers may be functional but they don't have
dedicated maintainers.

Missing monitor support means that the transport doesn't
implement events, and as such Flower, `celery events`, `celerymon`
and other event-based monitoring tools won't work.

Remote control means the ability to inspect and manage workers
at runtime using the `celery inspect` and `celery control` commands
(and other tools using the remote control API).

Summaries
=========

*Note: This section is not comprehensive of backends and brokers.*

Celery has the ability to communicate and store with many different backends (Result Stores) and brokers (Message Transports).

Redis
-----

Redis can be both a backend and a broker.

**As a Broker:** Redis works well for rapid transport of small messages. Large messages can congest the system.

:ref:`See documentation for details <broker-redis>`

**As a Backend:** Redis is a super fast K/V store, making it very efficient for fetching the results of a task call. As with the design of Redis, you do have to consider the limit memory available to store your data, and how you handle data persistence. If result persistence is important, consider using another DB for your backend.

RabbitMQ
--------

RabbitMQ is a broker.

**As a Broker:** RabbitMQ handles larger messages better than Redis, however if many messages are coming in very quickly, scaling can become a concern and Redis or SQS should be considered unless RabbitMQ is running at very large scale.

:ref:`See documentation for details <broker-rabbitmq>`

**As a Backend:** RabbitMQ can store results via ``rpc://`` backend. This backend creates separate temporary queue for each client.

*Note: RabbitMQ (as the broker) and Redis (as the backend) are very commonly used together. If more guaranteed long-term persistence is needed from the result store, consider using PostgreSQL or MySQL (through SQLAlchemy), Cassandra, or a custom defined backend.*

SQS
---

SQS is a broker.

If you already integrate tightly with AWS, and are familiar with SQS, it presents a great option as a broker. It is extremely scalable and completely managed, and manages task delegation similarly to RabbitMQ. It does lack some of the features of the RabbitMQ broker such as ``worker remote control commands``.

:ref:`See documentation for details <broker-sqs>`

SQLAlchemy
----------

SQLAlchemy is backend.

It allows Celery to interface with MySQL, PostgreSQL, SQlite, and more. It is a ORM, and is the way Celery can use a SQL DB as a result backend. Historically, SQLAlchemy has not been the most stable result backend so if chosen one should proceed with caution.
