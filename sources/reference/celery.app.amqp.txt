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

        .. attribute:: queues

            All currently defined task queues. (A :class:`Queues` instance).

        .. automethod:: ConsumerSet
        .. automethod:: Queues
        .. automethod:: Router
        .. automethod:: TaskConsumer
        .. automethod:: TaskPublisher

        .. automethod:: get_task_consumer
        .. automethod:: get_default_queue
        .. automethod:: get_broker_info
        .. automethod:: format_broker_info

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
