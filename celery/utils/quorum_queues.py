from __future__ import annotations


def detect_quorum_queues(app, driver_type: str) -> tuple[bool, str]:
    """Detect if any of the queues are quorum queues.

    Returns:
        tuple[bool, str]: A tuple containing a boolean indicating if any of the queues are quorum queues
        and the name of the first quorum queue found or an empty string if no quorum queues were found.
    """
    is_rabbitmq_broker = driver_type == 'amqp'

    if is_rabbitmq_broker:
        queues = app.amqp.queues
        for qname in queues:
            qarguments = queues[qname].queue_arguments or {}
            if qarguments.get("x-queue-type") == "quorum":
                return True, qname

    return False, ""
