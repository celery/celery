"""myapp.py

Usage::

   (window1)$ python myapp.py worker -l INFO

   (window2)$ celery shell
   >>> from myapp import example
   >>> example()


You can also specify the app to use with the `celery` command,
using the `-A` / `--app` option::

    $ celery -A myapp worker -l INFO

With the `-A myproj` argument the program will search for an app
instance in the module ``myproj``.  You can also specify an explicit
name using the fully qualified form::

    $ celery -A myapp:app worker -l INFO

"""

import os
from datetime import UTC, datetime, timedelta

from declare_queue import my_quorum_queue

from celery import Celery
from celery.canvas import group

app = Celery("myapp", broker="amqp://guest@localhost//")

# Use custom queue (Optional) or set the default queue type to "quorum"
# app.conf.task_queues = (my_quorum_queue,)  # uncomment to use custom queue
app.conf.task_default_queue_type = "quorum"  # comment to use classic queue

# Required by Quorum Queues: https://www.rabbitmq.com/docs/quorum-queues#use-cases
app.conf.broker_transport_options = {"confirm_publish": True}

# Reduce qos to 4 (Optional, useful for testing)
app.conf.worker_prefetch_multiplier = 1
app.conf.worker_concurrency = 4

# Reduce logs (Optional, useful for testing)
app.conf.worker_heartbeat = None
app.conf.broker_heartbeat = 0


def is_using_quorum_queues(app) -> bool:
    queues = app.amqp.queues
    for qname in queues:
        qarguments = queues[qname].queue_arguments or {}
        if qarguments.get("x-queue-type") == "quorum":
            return True

    return False


@app.task
def add(x, y):
    return x + y


@app.task
def identity(x):
    return x


def example():
    queue = my_quorum_queue.name if my_quorum_queue in (app.conf.task_queues or {}) else "celery"

    while True:
        print("Celery Quorum Queue Example")
        print("===========================")
        print("1. Send a simple identity task")
        print("1.1 Send an ETA identity task")
        print("2. Send a group of add tasks")
        print("3. Inspect the active queues")
        print("4. Shutdown Celery worker")
        print("Q. Quit")
        print("Q! Exit")
        choice = input("Enter your choice (1-4 or Q): ")

        if choice == "1" or choice == "1.1":
            queue_type = "Quorum" if is_using_quorum_queues(app) else "Classic"
            payload = f"Hello, {queue_type} Queue!"
            eta = datetime.now(UTC) + timedelta(seconds=30)
            if choice == "1.1":
                result = identity.si(payload).apply_async(queue=queue, eta=eta)
            else:
                result = identity.si(payload).apply_async(queue=queue)
            print()
            print(f"Task sent with ID: {result.id}")
            print("Task type: identity")

            if choice == "1.1":
                print(f"ETA: {eta}")

            print(f"Payload: {payload}")

        elif choice == "2":
            tasks = [
                (1, 2),
                (3, 4),
                (5, 6),
            ]
            result = group(
                add.s(*tasks[0]),
                add.s(*tasks[1]),
                add.s(*tasks[2]),
            ).apply_async(queue=queue)
            print()
            print("Group of tasks sent.")
            print(f"Group result ID: {result.id}")
            for i, task_args in enumerate(tasks, 1):
                print(f"Task {i} type: add")
                print(f"Payload: {task_args}")

        elif choice == "3":
            active_queues = app.control.inspect().active_queues()
            print()
            print("Active queues:")
            for worker, queues in active_queues.items():
                print(f"Worker: {worker}")
                for q in queues:
                    print(f"  - {q['name']}")

        elif choice == "4":
            print("Shutting down Celery worker...")
            app.control.shutdown()

        elif choice.lower() == "q":
            print("Quitting test()")
            break

        elif choice.lower() == "q!":
            print("Exiting...")
            os.abort()

        else:
            print("Invalid choice. Please enter a number between 1 and 4 or Q to quit.")

        print("\n" + "#" * 80 + "\n")


if __name__ == "__main__":
    app.start()
