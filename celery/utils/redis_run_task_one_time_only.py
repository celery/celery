from redis import Redis
from functools import wraps
from logging import error


def run_only_one_instance(redis_addr: str = "redis"):
    def real_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                sentinel = Redis(host=redis_addr).incr(func.__module__+func.__name__ + str(args) + str(kwargs))
                if sentinel == 1:
                    return func(*args, **kwargs)
                else:
                    pass
                Redis(host=redis_addr).decr(func.__module__+func.__name__ + str(args) + str(kwargs))
            except Exception as e:
                Redis(host=redis_addr).decr(func.__module__+func.__name__ + str(args) + str(kwargs))
                error(e)

        return wrapper

    return real_decorator
