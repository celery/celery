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

            Base Consumer class used.  Default is :class:`kombu.compat.Consumer`.

        .. attribute:: queues

            All currently defined task queues. (A :class:`Queues` instance).

        .. automethod:: Queues
        .. automethod:: Router
        .. autoattribute:: TaskConsumer
        .. autoattribute:: TaskProducer
        .. automethod:: flush_routes

        .. autoattribute:: default_queue
        .. autoattribute:: default_exchange
        .. autoattribute:: publisher_pool
        .. autoattribute:: router
        .. autoattribute:: routes

    Queues
    ------

    .. autoclass:: Queues

        .. automethod:: add

        .. automethod:: format

        .. automethod:: select_subset

        .. automethod:: new_missing

        .. autoattribute:: consume_from

    TaskPublisher
    -------------

    .. autoclass:: TaskPublisher
        :members:
        :undoc-members:
