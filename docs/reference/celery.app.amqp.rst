.. currentmodule:: celery.app.amqp

.. automodule:: celery.app.amqp

    .. contents::
        :local:

    AMQP
    ----

    .. autoclass:: AMQP

        .. attribute:: Connection

            Broker connection class used. Default is :class:`kombu.Connection`.

        .. attribute:: Consumer

            Base Consumer class used. Default is :class:`kombu.Consumer`.

        .. attribute:: Producer

            Base Producer class used. Default is :class:`kombu.Producer`.

        .. attribute:: queues

            All currently defined task queues (a :class:`Queues` instance).

        .. automethod:: Queues
        .. automethod:: Router
        .. automethod:: flush_routes

        .. autoattribute:: create_task_message
        .. autoattribute:: send_task_message
        .. autoattribute:: default_queue
        .. autoattribute:: default_exchange
        .. autoattribute:: producer_pool
        .. autoattribute:: router
        .. autoattribute:: routes

    Queues
    ------

    .. autoclass:: Queues
        :members:
        :undoc-members:
