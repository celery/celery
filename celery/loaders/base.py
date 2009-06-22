

class BaseLoader(object):
    _conf_cache = None

    def on_task_preinit(self, task_id, task):
        pass

    def on_worker_init(self):
        pass

    @property
    def conf(self):
        if not self._conf_cache:
            self._conf_cache = self.read_configuration()
        return self._conf_cache
