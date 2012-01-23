from multiprocessing.process import Process as _Process

from .forking import Popen


class Process(_Process):
    _Popen = Popen

    def __init__(self, *args, **kwargs):
        self.should_fork = kwargs.pop("should_fork", True)
        super(Process, self).__init__(*args, **kwargs)
