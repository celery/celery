try:
    import billiard
    from billiard import util
    from billiard import pool
    current_process = billiard.current_process
    register_after_fork = util.register_after_fork
    freeze_support = billiard.freeze_support
    Process = billiard.Process
    cpu_count = billiard.cpu_count
    Pool = pool.Pool
    RUN = pool.RUN
except ImportError:
    try:
        import multiprocessing
        from multiprocessing import util
        from multiprocessing import pool
        current_process = multiprocessing.current_process
        register_after_fork = util.register_after_fork
        freeze_support = multiprocessing.freeze_support
        Process = multiprocessing.Process
        cpu_count = multiprocessing.cpu_count
        Pool = pool.Pool
        RUN = pool.RUN
    except ImportError:
        current_process = None
        util = None
        register_after_fork = lambda *a, **kw: None
        freeze_support = lambda: True
        Process = None
        cpu_count = lambda: 2
        Pool = None
        RUN = 1


def get_process_name():
    if current_process is not None:
        return current_process().name

def forking_enable(enabled):
    try:
        from billiard import forking_enable
    except ImportError:
        try:
            from multiprocessing import forking_enable
        except ImportError:
            return
    forking_enable(enabled)
