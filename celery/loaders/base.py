BUILTIN_MODULES = ["celery.task"]


class BaseLoader(object):
    """The base class for loaders.

    Loaders handles to following things:

        * Reading celery client/worker configurations.

        * What happens when a task starts?
            See :meth:`on_task_init`.

        * What happens when the worker starts?
            See :meth:`on_worker_init`.

        * What modules are imported to find tasks?

    """
    _conf_cache = None
    worker_initialized = False
    override_backends = {}

    def on_task_init(self, task_id, task):
        """This method is called before a task is executed."""
        pass

    def on_worker_init(self):
        """This method is called when the worker (``celeryd``) starts."""
        pass

    def import_task_module(self, module):
        return __import__(module, [], [], [''])

    def import_default_modules(self):
        imports = getattr(self.conf, "CELERY_IMPORTS", None) or []
        imports = set(list(imports) + BUILTIN_MODULES)
        return map(self.import_task_module, imports)

    def init_worker(self):
        if not self.worker_initialized:
            self.worker_initialized = True
            self.on_worker_init()

    @property
    def conf(self):
        """Loader configuration."""
        if not self._conf_cache:
            self._conf_cache = self.read_configuration()
        return self._conf_cache
