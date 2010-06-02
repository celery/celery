import sys
import time
import curses
import atexit
import socket
import optparse
import threading

from datetime import datetime
from itertools import count

import celery
from celery import states
from celery.task import control
from celery.events import EventReceiver
from celery.events.state import State
from celery.messaging import establish_connection
from celery.datastructures import LocalCache

TASK_NAMES = LocalCache(0xFFF)
HUMAN_TYPES = {"worker-offline": "shutdown",
               "worker-online": "started",
               "worker-heartbeat": "heartbeat"}
OPTION_LIST = (
    optparse.make_option('-d', '--DUMP',
        action="store_true", dest="dump",
        help="Dump events to stdout."),
)



def humanize_type(type):
    try:
        return HUMAN_TYPES[type.lower()]
    except KeyError:
        return type.lower().replace("-", " ")


class Dumper(object):

    def on_event(self, event):
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
            return self.format_task_event(hostname, timestamp,
                                          type, task, event)
        fields = ", ".join("%s=%s" % (key, event[key])
                        for key in sorted(event.keys()))
        sep = fields and ":" or ""
        print("%s [%s] %s%s %s" % (hostname, timestamp,
                                    humanize_type(type), sep, fields))

    def format_task_event(self, hostname, timestamp, type, task, event):
        fields = ", ".join("%s=%s" % (key, event[key])
                        for key in sorted(event.keys()))
        sep = fields and ":" or ""
        print("%s [%s] %s%s %s %s" % (hostname, timestamp,
                                    humanize_type(type), sep, task, fields))


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
    win = None
    screen_width = None
    selected_task = None
    selected_position = 0
    selected_str = "Selected: "
    limit = 20
    foreground = curses.COLOR_BLACK
    background = curses.COLOR_WHITE
    online_str = "Workers online: "
    help_title = "Keys: "
    help = ("j:up k:down c:revoke t:traceback r:result q: quit")
    greet = "celeryev %s" % celery.__version__
    info_str = "Info: "

    def __init__(self, state):
        self.state = state

    def format_row(self, uuid, worker, task, time, state):
        my, mx = self.win.getmaxyx()
        mx = mx - 3
        uuid_max = 36
        if mx < 88:
            uuid_max = mx - 52 - 2
        uuid = abbr(uuid, uuid_max).ljust(uuid_max)
        worker = abbr(worker, 16).ljust(16)
        task = abbrtask(task, 16).ljust(16)
        state = abbr(state, 8).ljust(8)
        time = time.ljust(8)
        row = "%s %s %s %s %s " % (uuid, worker, task, time, state)
        if self.screen_width is None:
            self.screen_width = len(row[:mx])
        return row[:mx]

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
        elif key in ("C", ):
            self.revoke_selection()
        elif key in ("T", ):
            self.selection_traceback()
        elif key in ("Q", ):
            raise SystemExit

    def alert(self, callback):
        self.win.erase()
        my, mx = self.win.getmaxyx()
        callback(my, mx)
        self.win.addstr(my - 1, 0, "Press any key to continue...", curses.A_BOLD)
        self.win.refresh()
        while 1:
            try:
                return self.win.getkey().upper()
            except:
                pass

    def revoke_selection(self):
        control.revoke(self.selected_task)

    def selection_traceback(self):

        def alert_callback(my, mx):
            y = count(2).next
            task = self.state.tasks[self.selected_task]
            for line in task.traceback.split("\n"):
                self.win.addstr(y(), 3, line)

        return self.alert(alert_callback)


    def draw(self):
        win = self.win
        self.handle_keypress()
        x = 3
        y = blank_line = count(2).next
        my, mx = win.getmaxyx()
        win.erase()
        win.bkgd(" ", curses.color_pair(1))
        win.border()
        win.addstr(1, x, self.greet, curses.A_DIM | curses.color_pair(5))
        blank_line()
        win.addstr(y(), x, self.format_row("UUID", "TASK",
                                           "WORKER", "TIME", "STATE"),
                curses.A_BOLD | curses.A_UNDERLINE)
        tasks = self.tasks
        if tasks:
            for uuid, task in tasks:
                if task.uuid:
                    state_color = self.state_colors.get(task.state)
                    attr = curses.A_NORMAL
                    if task.uuid == self.selected_task:
                        attr = curses.A_STANDOUT
                    timestamp = datetime.fromtimestamp(task.timestamp)
                    timef = timestamp.strftime("%H:%M:%S")
                    line = self.format_row(uuid, task.name,
                                           task.worker.hostname,
                                           timef, task.state)
                    lineno = y()
                    win.addstr(lineno, x, line, attr)
                    if state_color:
                        win.addstr(lineno, len(line) - len(task.state) + 1,
                                task.state, state_color | attr)
                    if task.ready:
                        task.visited = time.time()

        # -- Footer
        blank_line()
        win.hline(my - 6, x, curses.ACS_HLINE, self.screen_width)

        # Selected Task Info
        if self.selected_task:
            win.addstr(my - 5, x, self.selected_str, curses.A_BOLD)
            info = "Missing extended info"
            try:
                selection = self.state.tasks[self.selected_task]
            except KeyError:
                pass
            else:
                info = selection.info
                if "runtime" in info:
                    info["runtime"] = "%.2fs" % info["runtime"]
                if "result" in info:
                    info["result"] = abbr(result["info"], 16)
                info = " ".join("%s=%s" % (key, value)
                            for key, value in info.items())
            win.addstr(my - 5, x + len(self.selected_str), info)
        else:
            win.addstr(my - 5, x, "No task selected", curses.A_NORMAL)


        # Workers
        if self.workers:
            win.addstr(my - 4, x, self.online_str, curses.A_BOLD)
            win.addstr(my - 4, x + len(self.online_str),
                    ", ".join(self.workers), curses.A_NORMAL)
        else:
            win.addstr(my - 4, x, "No workers discovered.")

        # Info
        win.addstr(my - 3, x, self.info_str, curses.A_BOLD)
        win.addstr(my - 3, x + len(self.info_str),
                "events:%s tasks:%s workers:%s/%s" % (
                    self.state.event_count, self.state.task_count,
                    len([w for w in self.state.workers.values()
                            if w.alive]),
                    len(self.state.workers)),
                curses.A_DIM)

        # Help
        win.addstr(my - 2, x, self.help_title, curses.A_BOLD)
        win.addstr(my - 2, x + len(self.help_title), self.help, curses.A_DIM)
        win.refresh()

    def init_screen(self):
        self.win = curses.initscr()
        self.win.nodelay(True)
        self.win.keypad(True)
        curses.start_color()
        curses.init_pair(1, self.foreground, self.background)
        # exception states
        curses.init_pair(2, curses.COLOR_RED, self.background)
        # successful state
        curses.init_pair(3, curses.COLOR_GREEN, self.background)
        # revoked state
        curses.init_pair(4, curses.COLOR_MAGENTA, self.background)
        # greeting
        curses.init_pair(5, curses.COLOR_BLUE, self.background)

        self.state_colors = {states.SUCCESS: curses.color_pair(3),
                             states.REVOKED: curses.color_pair(4)}
        for state in states.EXCEPTION_STATES:
            self.state_colors[state] = curses.color_pair(2)

        curses.cbreak()

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


def eventtop():
    sys.stderr.write("-> celeryev: starting capture...\n")
    state = State()
    display = CursesMonitor(state)
    display.init_screen()
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


def eventdump():
    sys.stderr.write("-> celeryev: starting capture...\n")
    dumper = Dumper()
    conn = establish_connection()
    recv = EventReceiver(conn, handlers={"*": dumper.on_event})
    try:
        recv.capture()
    except (KeyboardInterrupt, SystemExit):
        conn and conn.close()


def run_celeryev(dump=False):
    if dump:
        return eventdump()
    return eventtop()


def parse_options(arguments):
    """Parse the available options to ``celeryev``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def main():
    options = parse_options(sys.argv[1:])
    return run_celeryev(**vars(options))





if __name__ == "__main__":
    main()
