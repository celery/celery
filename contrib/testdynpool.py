from billiard.pool import DynamicPool
from multiprocessing import get_logger, log_to_stderr
import logging


def setup_logger():
    log_to_stderr()
    logger = get_logger()
    logger.setLevel(logging.DEBUG)
    return logger


def target(n):
    r = n * n
    setup_logger().info("%d * %d = %d" % (n, n, r))
    return r


def exit_process():
    setup_logger().error("EXITING NOW!")
    import os
    os._exit(0)


def send_exit(pool):
    pool.apply_async(exit_process)


def do_work(pool):
    results = [pool.apply_async(target, args=[i]) for i in range(10)]
    [result.get() for result in results]


def workpool():
    pool = DynamicPool(2)
    do_work(pool)
    print("GROWING")
    pool.grow(1)
    do_work(pool)
    send_exit(pool)
    import time
    time.sleep(2)
    pool.replace_dead_workers()
    do_work(pool)


if __name__ == "__main__":
    workpool()
