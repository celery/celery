from __future__ import absolute_import

from multiprocessing.process import Process as _Process

from .forking import Popen


class Process(_Process):
    _Popen = Popen

    def __init__(self, *args, **kwargs):
        self.force_execv = kwargs.pop("force_execv", False)
        super(Process, self).__init__(*args, **kwargs)
