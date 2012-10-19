.. currentmodule:: celery.app.amqp

.. automodule:: celery.app.amqp

    .. contents::
        :local:

    AMQP
    ----

    .. autoclass:: AMQP

        .. attribute:: Connection

            Broker connection class used.  Default is
            :class:`kombu.connection.Connection`.

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
        :members:
        :undoc-members:

    TaskPublisher
    -------------

    .. autoclass:: TaskPublisher
        :members:
        :undoc-members:
