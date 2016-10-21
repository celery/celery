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
