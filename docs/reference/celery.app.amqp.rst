.. currentmodule:: celery.app.amqp

.. automodule:: celery.app.amqp

    .. contents::
        :local:

    AMQP
    ----

    .. autoclass:: AMQP

        .. attribute:: BrokerConnection

            Broker connection class used.  Default is
            :class:`kombu.connection.BrokerConnection`.

        .. attribute:: Consumer

            The task consumer class used.
            Default is :class:`kombu.compat.Consumer`.

        .. attribute:: ConsumerSet

            The class used to consume from multiple queues using the
            same channel.

        .. attribute:: queues

            All currently defined task queues. (A :class:`Queues` instance).

        .. automethod:: Queues
        .. automethod:: Router
        .. automethod:: TaskConsumer
        .. automethod:: TaskPublisher

        .. automethod:: get_task_consumer
        .. automethod:: get_default_queue

    Queues
    ------

    .. autoclass:: Queues

        .. automethod:: add

        .. automethod:: options

        .. automethod:: format

        .. automethod:: select_subset

        .. automethod:: with_defaults

    TaskPublisher
    -------------

    .. autoclass:: TaskPublisher
        :members:
        :undoc-members:
