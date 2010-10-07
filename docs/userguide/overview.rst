.. _guide-overview:

==========
 Overview
==========

.. contents::
    :local:

.. _overview-figure-1:

.. figure:: ../images/celery-broker-worker-nodes.jpg

    *Figure 1:* Worker and broker nodes.

To use Celery you need at least two main components; a message broker and
a worker.

The message broker enables clients and workers to communicate through
messaging.  There are several broker implementations available, the most
popular being RabbitMQ.

The worker processes messages, and consists of one or more physical (or virtual)
nodes.


Tasks
=====

The action to take whenever a message of a certain type is received is called
a "task".

* Go to :ref:`guide-tasks`.
* Go to :ref:`guide-executing`.
* Go to :ref:`guide-sets`
* Go to :ref:`guide-beat`.
* Go to :ref:`guide-webhooks`.


Workers
=======
Go to :ref:`guide-worker`.

Monitoring
==========
Go to :ref:`guide-monitoring`.

Routing
=======

.. _overview-figure-2:

.. figure:: ../images/celery-worker-bindings.jpg

    *Figure 2:* Worker bindings.

Go to :ref:`guide-routing`.

Celery takes advantage of AMQPs flexible routing model.  Tasks can be routed
to specific servers, or a cluster of servers by binding workers to different
queues. A single worker node can be bound to one or more queues.
Multiple messaging scenarios are supported: round robin, point-to-point,
broadcast (one-to-many), and more.

Celery aims to hide the complexity of AMQP through features like
:ref:`routing-automatic`, while still preserving the ability to go
low level if that should be necessary.

