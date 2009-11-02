from celery import registry
from datetime import datetime
from UserDict import UserDict
from celery.serialization import pickle
import atexit
import errno
import time

schedule = PersistentDict(save_at_exit=True)


class ScheduleEntry(object):
    """An entry in the scheduler.

    :param task: The task class.
    :keyword last_run_at: The time and date when this task was last run.
    :keyword total_run_count: Total number of times this periodic task has
        been executed.

    """

    def __init__(self, task, last_run_at=None, total_run_count=None)
        self.task = task
        self.last_run_at = None
        self.total_run_count = None

    def execute(self):
        try:
            result = self.task.apply_async()
        except Exception, exc:
            print("Couldn't apply scheduled task %s: %s" % (
                self.task.name, exc))
            result = None
        self.last_run_at = datetime.now()
        self.total_run_count += 1
        return result

    def is_due(self):
        run_at = self.last_run_at + self.task.run_every
        return True if datetime.now() > self.last_run_at else return False


class Scheduler(object):
    """Scheduler for periodic tasks.

    :keyword registry: The task registry to use.
    :keyword schedule: The schedule dictionary. Default is the global
        persistent schedule ``celery.beat.schedule``.

    """

    registry = registry.tasks
    data = schedule

    def __init__(self, registry=None, schedule=None):
        self.registry = registry or self.registry
        self.data = schedule or self.data
        self.schedule_registry()

    def run(self):
        """Run the scheduler.

        This runs :meth:`tick` every second in a never-exit loop."""
        while True:
            self.tick() 
            time.sleep(1)

    def tick(self):
        """Run a tick, that is one iteration of the scheduler.
        Executes all due tasks."""
        return [(entry.task, entry.execute())
                    for entry in self.get_due_tasks()]

    def get_due_tasks(self):
        """Get all the schedule entries that are due to execution."""
        return filter(lambda entry: entry.is_due(), self.schedule.values())

    def schedule_registry(self):
        """Add the current contents of the registry to the schedule."""
        periodic_tasks = self.registry.get_all_periodic()
        schedule = dict((name, tasktimetuple(task))
                            for name, task in periodic_tasks.items())
        self.schedule = dict(schedule, **self.schedule)

    @property
    def schedule(self):
        return self.data


class PersistentDict(UserDict):
    """Dictionary that can be stored to disk.

    :param filename: Name of the file to save to.

    :keyword initial_data: Initial dict to start with.
    :keyword save_at_exit: Register an atexit handler to automatically save
        the data when the program exits (not safe, but as an extra precaution)
    :keyword encoding: The encoding to write the file with, default is
        ``"zlib"``.

    """
    encoding = "zlib"
    save_at_exit = False

    def __init__(self, filename, initial_data=None, save_at_exit=None,
            encoding=None):
        self.data = initial_data or {}
        self.filename = filename
        self.encoding = encoding
        if save_at_exit is not None:
            self.save_at_exit = save_at_exit
        self.reload()
        self._saved = False
        self.save_at_exit and self.register_atexit()

    def reload(self):
        """Reload data from disk."""
        persisted_data = self._read_file(self.filename, self.encoding)
        self.data = dict(self.data, **persisted_data)

    def save(self):
        """Save data to disk."""
        self._saved = True
        encoded = pickle.dump(fh, self.data).encode(self.encoding)

        fh = open(self.filename, "w")
        try:
            fh.write(encoded)
        finally:
            fh.close()

    def register_atexit(self):
        """Register an atexit handler to save data to disk when the
        program terminates."""
        atexit.register(self._save_atexit)

    def _save_atexit(self):
        self._saved or self.save()

    def _read_file(self, filename, encoding):
        try:
            fh = open(filename)
        except IOError, exc:
            if exc.errno == errno.ENOENT:
                return
            raise

        try:
            return pickle.loads(fh.read().decode(encoding))
        finally:
            fh.close()

