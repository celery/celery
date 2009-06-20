

class Loader(object):

    def __init__(self):
        self._conf_cache = None

    def read_configuration(self):
        return dict()

    def on_task_preinit(self, task_id, task):
        pass

    def on_worker_init(self):
        imports = self.conf.get("imports", [])
        for module in imports:
            __import__(module, [], [], {''})

    @property
    def conf(self):
        if not self._conf_cache:
            self._conf_cache = self.read_configuration()
        return self._conf_cache

