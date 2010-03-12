===============
 Routing Tasks
===============

**NOTE** This document refers to functionality only available in brokers
using AMQP. Other brokers may implement some functionality, see their
respective documenation for more information, or contact the `mailinglist`_.

.. _`mailinglist`: http://groups.google.com/group/celery-users

AMQP Primer
===========

Exchanges, queues and routing keys.
-----------------------------------
TODO Mindblowing one-line simple explanation here. TODO

The steps required to send and receive messages are:

    1. Create an exchange
    2. Create a queue
    3. Bind the queue to the exchange.

Exchange type
-------------

The exchange type defines how the messages are routed through the exchange.
The exchanges defined in the standard is ``direct``, ``topic``, ``fanout`` and
``headers``. Also non-standard exchange types available as plugins to RabbitMQ, like
the last value cache plug-in.


Consumers and Producers
-----------------------
TODO

Related API commands
-------------------------

* exchange.declare(exchange_name, type, passive, durable, auto_delete, internal)

Declares an exchange by name.

    * ``passive`` means the exchange won't be created, but you can use this to
      check if the exchange already exists.

    * Durable exchanges are persistent. I.e. they survive a broker restart.

    * ``auto_delete`` means the queue will be deleted by the broker when there
      are no more queues using it.

* queue.declare(queue_name, passive, durable, exclusive, auto_delete)

Declares a queue by name.

    * exclusive queues can only be consumed from by the current connection.
      implies ``auto_delete``.

* queue.bind(queue_name, exchange_name, routing_key)

Binds a queue to an exchange with a routing key.
Unbound queues will not receive messages, so this is necessary.

* queue.delete(name, if_unused, if_empty)

Deletes a queue and its binding.

* exchange.delete(name, if_unused)

Deletes an exchange.

**NOTE**: Declaring does not necessarily mean "create". When you declare you
*assert* that the entity exists and it is operable. There is no rule as to
whom should initially create the exchange/queue/binding, whether consumer
or producer.  Usually the first one to need it will create it.


Hands-on with the API
---------------------

Celery comes with a tool called ``camqadm`` (short for celery AMQP admin).
It's used for simple admnistration tasks like deleting queues/exchanges,
purging queues and creating queue entities. In short it's for simple command
line access to the AMQP API.

You can write commands directly in the arguments to ``camqadm``, or just start
with no arguments which makes it start in shell-mode::

    $ camqadm
    -> connecting to amqp://guest@localhost:5672/.
    -> connected.
    -->

Here, ``-->`` is the prompt. Type ``help`` for a list of commands, there's
also autocomplete so you can start typing a command then hit ``tab`` to show a
list of possible matches.

Now let's create a queue we can send messages to::

    --> exchange.declare testexchange direct
    ok.
    --> queue.declare testqueue
    ok. queue:testqueue messages:0 consumers:0.
    --> queue.bind testqueue testexchange testkey
    ok.


Messages are sent with a routing key, to an exchange. This is done using
the ``basic.publish`` command::

    --> basic.publish "This is a message!" testexchange testkey
    ok.


Now that the message is sent we can retrieve it again, we use the
``basic.get`` command here, which pops a single message off the queue,
this command is not recommended for production as it implies polling, any
real application would declare consumers instead::

    --> basic.get testqueue
    {'body': 'This is a message!',
     'delivery_info': {'delivery_tag': 1,
                       'exchange': u'testexchange',
                       'message_count': 0,
                       'redelivered': False,
                       'routing_key': u'testkey'},
     'properties': {}}


AMQP uses acknowledgment to signify a message has been received and processed
successfully. The message is sent to the next receiver if the client
connection is closed, and it has not yet been acknowledged.

Note the delivery tag listed in the structure above; Within a connection channel,
every received message has a unique delivery tag,
This tag is used to acknowledge the message. Note that
delivery tags are not unique across connections, so in another client
the delivery tag ``1`` might point to a different message than in our channel.

You can acknowledge the message we received using ``basic.ack``::

    --> basic.ack 1
    ok.


To clean up after ourselves we should delete the entities we just created::

    --> queue.delete testqueue
    ok. 0 messages deleted.
    --> exchange.delete testexchange
    ok.





