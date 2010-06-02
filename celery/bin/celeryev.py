import sys
import curses

from datetime import datetime
from itertools import count

from celery.events import EventReceiver
from celery.events.state import State
from celery.messaging import establish_connection
from celery.datastructures import LocalCache

TASK_NAMES = LocalCache(0xFFF)
HUMAN_TYPES = {"worker-offline": "shutdown",
               "worker-online": "started",
               "worker-heartbeat": "heartbeat"}


def humanize_type(type):
    try:
        return HUMAN_TYPES[type.lower()]
    except KeyError:
        return type.lower().replace("-", " ")


def dump_event(event):
    timestamp = datetime.fromtimestamp(event.pop("timestamp"))
    type = event.pop("type").lower()
    hostname = event.pop("hostname")
    if type.startswith("task-"):
        uuid = event.pop("uuid")
        if type.startswith("task-received"):
            task = TASK_NAMES[uuid] = "%s(%s) args=%s kwargs=%s" % (
                    event.pop("name"), uuid,
                    event.pop("args"),
                    event.pop("kwargs"))
        else:
            task = TASK_NAMES.get(uuid, "")
        return format_task_event(hostname, timestamp, type, task, event)
    fields = ", ".join("%s=%s" % (key, event[key])
                    for key in sorted(event.keys()))
    sep = fields and ":" or ""
    print("%s [%s] %s%s %s" % (hostname, timestamp,
                                humanize_type(type), sep, fields))


def format_task_event(hostname, timestamp, type, task, event):
    fields = ", ".join("%s=%s" % (key, event[key])
                    for key in sorted(event.keys()))
    sep = fields and ":" or ""
    print("%s [%s] %s%s %s %s" % (hostname, timestamp,
                                humanize_type(type), sep, task, fields))

def eventdump():
    sys.stderr.write("-> celeryev: starting capture...\n")
    conn = establish_connection()
    recv = EventReceiver(conn, handlers={"*": dump_event})
    try:
        recv.capture()
    except (KeyboardInterrupt, SystemExit):
        conn and conn.close()


def main():
    sys.stderr.write("-> celeryev: starting capture...\n")
    stdscr = curses.initscr()
    curses.cbreak()

    i = count(0).next
    my, mx = stdscr.getmaxyx()
    def callback(state, event):
        workers = []
        for worker in state.workers.values():
            if worker.hostname:
                workers.append("%s %s" % (worker.hostname,
                    worker.alive and "online" or "offline"))
        tasks = []
        for uuid, task in state.tasks_by_timestamp():
            if task.uuid and not task.visited:
                tasks.append("%s %s %s" % (task.uuid,
                    task.name, task.state))
                if task.ready:
                    task.visited = True
        for i, line in enumerate(workers):
            stdscr.addstr(i, 0, line, curses.A_REVERSE)
            stdscr.addstr(i, len(line), " " * (my - len(line)),
                    curses.A_DIM)
        stdscr.addstr(i + 1, 0, " " * 32, curses.A_UNDERLINE)
        stdscr.addstr(i + 2, 0, " " * 32, curses.A_DIM)
        for j, line in enumerate(tasks):
            line = line + "    "
            line = line + " " * (my - len(line))
            stdscr.addstr(j + i + 3, 0, line, curses.A_DIM)
            stdscr.addstr(i, len(line), " " * (my - len(line)),
                    curses.A_DIM)
        stdscr.refresh()

    conn = establish_connection()
    state = State(callback)
    recv = EventReceiver(conn, handlers={"*": state.event})
    try:
        recv.capture()
    except (KeyboardInterrupt, SystemExit):
        conn and conn.close()
        curses.nocbreak()
        curses.echo()



if __name__ == "__main__":
    main()
