import sys
import time
import curses
import atexit
import socket
import threading

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



def abbr(S, max, dots=True):
    if S is None:
        return "???"
    if len(S) > max:
        return dots and S[:max-3] + "..." or S[:max-3]
    return S


def abbrtask(S, max):
    if S is None:
        return "???"
    if len(S) > max:
        module, _, cls = rpartition(S, ".")
        module = abbr(module, max - len(cls), False)
        return module + "[.]" + cls
    return S


class CursesMonitor(object):
    screen_width = None
    selected_task = None
    selected_position = 0
    selected_str = "Selected: "
    limit = 20
    foreground = curses.COLOR_BLACK
    background = curses.COLOR_WHITE
    online_str = "Workers online: "

    def __init__(self, state):
        self.state = state

    def format_row(self, uuid, task, worker, state):
        uuid = uuid.ljust(36)
        worker = abbr(worker, 16).ljust(16)
        task = abbrtask(task, 20).ljust(30)
        state = abbr(state, 8).ljust(8)
        row = " %s %s %s %s " % (uuid, worker, task, state)
        if self.screen_width is None:
            self.screen_width = len(row)
        return row

    def find_position(self):
        if not self.tasks:
            return 0
        for i, e in enumerate(self.tasks):
            if self.selected_task == e[0]:
                return i
        return 0

    def move_selection(self, reverse=False):
        if not self.tasks:
            return
        incr = reverse and -1 or 1
        pos = self.find_position() + incr
        try:
            self.selected_task = self.tasks[pos][0]
        except IndexError:
            self.selected_task = self.tasks[0][0]

    def handle_keypress(self):
        try:
            key = self.win.getkey().upper()
        except:
            return
        if key in ("KEY_DOWN", "J"):
            self.move_selection()
        elif key in ("KEY_UP", "K"):
            self.move_selection(reverse=True)
        elif key in ("Q", ):
            raise KeyboardInterrupt

    def draw(self):
        self.handle_keypress()
        win = self.win
        x = 3
        y = blank_line = count(2).next
        my, mx = win.getmaxyx()
        win.erase()
        win.bkgd(" ", curses.color_pair(1))
        win.border()
        win.addstr(y(), x, self.format_row("UUID", "TASK", "WORKER", "STATE"),
                curses.A_STANDOUT)
        for uuid, task in self.tasks:
            if task.uuid:
                attr = curses.A_NORMAL
                if task.uuid == self.selected_task:
                    attr = curses.A_STANDOUT
                win.addstr(y(), x, self.format_row(uuid,
                    task.name, task.worker.hostname, task.state),
                    attr)
                if task.ready:
                    task.visited = time.time()

        if self.selected_task:
            win.addstr(my - 3, x, self.selected_str, curses.A_BOLD)
            info = "Missing extended info"
            try:
                selection = self.state.tasks[self.selected_task]
            except KeyError:
                pass
            else:
                info = selection.info
                if "runtime" in info:
                    info["runtime"] = "%.2fs" % info["runtime"]
                info = " ".join("%s=%s" % (key, value)
                            for key, value in info.items())
            win.addstr(my - 3, x + len(self.selected_str), info)

        else:
            win.addstr(my - 3, x, "No task selected", curses.A_NORMAL)
        if self.workers:
            win.addstr(my - 2, x, self.online_str, curses.A_BOLD)
            win.addstr(my - 2, x + len(self.online_str),
                    ", ".join(self.workers), curses.A_NORMAL)
        else:
            win.addstr(my - 2, x, "No workers discovered.")
        win.refresh()

    def setupscreen(self):
        self.win = curses.initscr()
        curses.start_color()
        curses.init_pair(1, self.foreground, self.background)
        curses.cbreak()
        self.win.nodelay(True)
        self.win.keypad(True)

    def resetscreen(self):
        curses.nocbreak()
        curses.echo()
        curses.endwin()

    @property
    def tasks(self):
        return self.state.tasks_by_timestamp()[:self.limit]

    @property
    def workers(self):
        return [hostname
                    for hostname, w in self.state.workers.items()
                        if w.alive]


class DisplayThread(threading.Thread):

    def __init__(self, display):
        self.display = display
        self.shutdown = False
        threading.Thread.__init__(self)

    def run(self):
        while not self.shutdown:
            self.display.draw()

def main():
    sys.stderr.write("-> celeryev: starting capture...\n")
    state = State()
    display = CursesMonitor(state)
    display.setupscreen()
    refresher = DisplayThread(display)
    refresher.start()
    conn = establish_connection()
    recv = EventReceiver(conn, handlers={"*": state.event})
    try:
        consumer = recv.consumer()
        consumer.consume()
        while True:
            try:
                conn.connection.drain_events()
            except socket.timeout:
                pass
    except Exception, exc:
        refresher.shutdown = True
        refresher.join()
        display.resetscreen()
        raise
    except (KeyboardInterrupt, SystemExit):
        conn and conn.close()
        refresher.shutdown = True
        refresher.join()
        display.resetscreen()





if __name__ == "__main__":
    main()
