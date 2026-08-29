"""Create a quorum queue using Kombu."""

from kombu import Connection, Exchange, Queue

my_quorum_queue = Queue(
    "my-quorum-queue",
    Exchange("default"),
    routing_key="default",
    queue_arguments={"x-queue-type": "quorum"},
)

with Connection("amqp://guest@localhost//") as conn:
    channel = conn.channel()
    my_quorum_queue.maybe_bind(conn)
    my_quorum_queue.declare()
