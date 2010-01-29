from django.dispatch import Signal

task_sent = Signal(providing_args=["task_id", "task",
                                   "args", "kwargs",
                                   "eta", "taskset"])

task_prerun = Signal(providing_args=["task_id", "task",
                                     "args", "kwargs"])

task_postrun = Signal(providing_args=["task_id", "task",
                                      "args", "kwargs", "retval"])

worker_init = Signal(providing_args=[])
worker_ready = Signal(providing_args=[])
worker_shutdown = Signal(providing_args=[])
