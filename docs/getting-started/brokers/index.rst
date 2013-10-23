.. _brokers:

=====================
 Brokers
=====================

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

Experimental Transports
=======================

.. toctree::
    :maxdepth: 1

    sqlalchemy
    django
    mongodb
    sqs
    couchdb
    beanstalk
    ironmq

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
| *Mongo DB*    | Experimental | Yes            | Yes                |
+---------------+--------------+----------------+--------------------+
| *Beanstalk*   | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Amazon SQS*  | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Couch DB*    | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Zookeeper*   | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Django DB*   | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *SQLAlchemy*  | Experimental | No             | No                 |
+---------------+--------------+----------------+--------------------+
| *Iron MQ*     | 3rd party    | No             | No                 |
+---------------+--------------+----------------+--------------------+

Experimental brokers may be functional but they do not have
dedicated maintainers.

Missing monitor support means that the transport does not
implement events, and as such Flower, `celery events`, `celerymon`
and other event-based monitoring tools will not work.

Remote control means the ability to inspect and manage workers
at runtime using the `celery inspect` and `celery control` commands
(and other tools using the remote control API).
