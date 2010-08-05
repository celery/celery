import sys
import time
import curses
import socket
import optparse
import threading

from datetime import datetime
from textwrap import wrap
from itertools import count

from carrot.utils import rpartition

import celery
from celery import states
from celery.task import control
from celery.events import EventReceiver
from celery.events.state import State
from celery.messaging import establish_connection
from celery.datastructures import LocalCache
from celery.utils import instantiate

TASK_NAMES = LocalCache(0xFFF)

HUMAN_TYPES = {"worker-offline": "shutdown",
               "worker-online": "started",
               "worker-heartbeat": "heartbeat"}

OPTION_LIST = (
    optparse.make_option('-d', '--DUMP',
        action="store_true", dest="dump",
        help="Dump events to stdout."),
    optparse.make_option('-c', '--camera',
        action="store", dest="camera",
        help="Camera class to take event snapshots with."),
    optparse.make_option('-f', '--frequency', '--freq',
        action="store", dest="frequency", type="float", default=1.0,
        help="Recording: Snapshot frequency."),
    optparse.make_option('-x', '--verbose',
        action="store_true", dest="verbose",
        help="Show more output.")
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
    keymap = {}
    win = None
    screen_width = None
    screen_delay = 0.1
    selected_task = None
    selected_position = 0
    selected_str = "Selected: "
    limit = 20
    foreground = curses.COLOR_BLACK
    background = curses.COLOR_WHITE
    online_str = "Workers online: "
    help_title = "Keys: "
    help = ("j:up k:down i:info t:traceback r:result c:revoke ^c: quit")
    greet = "celeryev %s" % celery.__version__
    info_str = "Info: "

    def __init__(self, state, keymap=None):
        self.keymap = keymap or self.keymap
        self.state = state
        default_keymap = {"J": self.move_selection_down,
                          "K": self.move_selection_up,
                          "C": self.revoke_selection,
                          "T": self.selection_traceback,
                          "R": self.selection_result,
                          "I": self.selection_info,
                          "L": self.selection_rate_limit}
        self.keymap = dict(default_keymap, **self.keymap)

    def format_row(self, uuid, worker, task, timestamp, state):
        my, mx = self.win.getmaxyx()
        mx = mx - 3
        uuid_max = 36
        if mx < 88:
            uuid_max = mx - 52 - 2
        uuid = abbr(uuid, uuid_max).ljust(uuid_max)
        worker = abbr(worker, 16).ljust(16)
        task = abbrtask(task, 16).ljust(16)
        state = abbr(state, 8).ljust(8)
        timestamp = timestamp.ljust(8)
        row = "%s %s %s %s %s " % (uuid, worker, task, timestamp, state)
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

    def move_selection_up(self):
        self.move_selection(-1)

    def move_selection_down(self):
        self.move_selection(1)

    def move_selection(self, direction=1):
        if not self.tasks:
            return
        pos = self.find_position()
        try:
            self.selected_task = self.tasks[pos + direction][0]
        except IndexError:
            self.selected_task = self.tasks[0][0]

    keyalias = {curses.KEY_DOWN: "J",
                curses.KEY_UP: "K",
                curses.KEY_ENTER: "I"}

    def handle_keypress(self):
        try:
            key = self.win.getkey().upper()
        except:
            return
        key = self.keyalias.get(key) or key
        handler = self.keymap.get(key)
        if handler is not None:
            handler()

    def alert(self, callback, title=None):
        self.win.erase()
        my, mx = self.win.getmaxyx()
        y = blank_line = count(2).next
        if title:
            self.win.addstr(y(), 3, title, curses.A_BOLD | curses.A_UNDERLINE)
            blank_line()
        callback(my, mx, y())
        self.win.addstr(my - 1, 0, "Press any key to continue...",
                        curses.A_BOLD)
        self.win.refresh()
        while 1:
            try:
                return self.win.getkey().upper()
            except:
                pass

    def selection_rate_limit(self):
        if not self.selected_task:
            return curses.beep()
        task = self.state.tasks[self.selected_task]
        if not task.name:
            return curses.beep()

        my, mx = self.win.getmaxyx()
        r = "New rate limit: "
        self.win.addstr(my - 2, 3, r, curses.A_BOLD | curses.A_UNDERLINE)
        self.win.addstr(my - 2, len(r) + 3, " " * (mx - len(r)))
        rlimit = self.readline(my - 2, 3 + len(r))

        if rlimit:
            reply = control.rate_limit(task.name, rlimit.strip(), reply=True)
            self.alert_remote_control_reply(reply)

    def alert_remote_control_reply(self, reply):

        def callback(my, mx, xs):
            y = count(xs).next
            if not reply:
                self.win.addstr(y(), 3, "No replies received in 1s deadline.",
                        curses.A_BOLD + curses.color_pair(2))
                return

            for subreply in reply:
                curline = y()

                host, response = subreply.items()[0]
                host = "%s: " % host
                self.win.addstr(curline, 3, host, curses.A_BOLD)
                attr = curses.A_NORMAL
                text = ""
                if "error" in response:
                    text = response["error"]
                    attr |= curses.color_pair(2)
                elif "ok" in response:
                    text = response["ok"]
                    attr |= curses.color_pair(3)
                self.win.addstr(curline, 3 + len(host), text, attr)

        return self.alert(callback, "Remote Control Command Replies")

    def readline(self, x, y):
        buffer = str()
        curses.echo()
        try:
            i = 0
            while True:
                ch = self.win.getch(x, y + i)
                if ch != -1:
                    if ch in (10, curses.KEY_ENTER): # enter
                        break
                    if ch in (27, ):
                        buffer = str()
                        break
                    buffer += chr(ch)
                    i += 1
        finally:
            curses.noecho()
        return buffer

    def revoke_selection(self):
        if not self.selected_task:
            return curses.beep()
        reply = control.revoke(self.selected_task, reply=True)
        self.alert_remote_control_reply(reply)

    def selection_info(self):
        if not self.selected_task:
            return

        def alert_callback(mx, my, xs):
            y = count(xs).next
            task = self.state.tasks[self.selected_task]
            info = task.info(extra=["state"])
            infoitems = [("args", info.pop("args", None)),
                         ("kwargs", info.pop("kwargs", None))] + info.items()
            for key, value in infoitems:
                if key is None:
                    continue
                curline = y()
                keys = key + ": "
                self.win.addstr(curline, 3, keys, curses.A_BOLD)
                wrapped = wrap(str(value), mx - 2)
                if len(wrapped) == 1:
                    self.win.addstr(curline, len(keys) + 3, wrapped[0])
                else:
                    for subline in wrapped:
                        self.win.addstr(y(), 3, " " * 4 + subline,
                                curses.A_NORMAL)

        return self.alert(alert_callback,
                "Task details for %s" % self.selected_task)

    def selection_traceback(self):
        if not self.selected_task:
            return curses.beep()
        task = self.state.tasks[self.selected_task]
        if task.state not in states.EXCEPTION_STATES:
            return curses.beep()

        def alert_callback(my, mx, xs):
            y = count(xs).next
            for line in task.traceback.split("\n"):
                self.win.addstr(y(), 3, line)

        return self.alert(alert_callback,
                "Task Exception Traceback for %s" % self.selected_task)

    def selection_result(self):
        if not self.selected_task:
            return

        def alert_callback(my, mx, xs):
            y = count(xs).next
            task = self.state.tasks[self.selected_task]
            result = getattr(task, "result", None) or getattr(task,
                    "exception", None)
            for line in wrap(result, mx - 2):
                self.win.addstr(y(), 3, line)

        return self.alert(alert_callback,
                "Task Result for %s" % self.selected_task)

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
                    timestamp = datetime.fromtimestamp(
                                    task.timestamp or time.time())
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
                info = selection.info(["args", "kwargs",
                                       "result", "runtime", "eta"])
                if "runtime" in info:
                    info["runtime"] = "%.2fs" % info["runtime"]
                if "result" in info:
                    info["result"] = abbr(info["result"], 16)
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
        # started state
        curses.init_pair(6, curses.COLOR_YELLOW, self.foreground)

        self.state_colors = {states.SUCCESS: curses.color_pair(3),
                             states.REVOKED: curses.color_pair(4),
                             states.STARTED: curses.color_pair(6)}
        for state in states.EXCEPTION_STATES:
            self.state_colors[state] = curses.color_pair(2)

        curses.cbreak()

    def resetscreen(self):
        curses.nocbreak()
        self.win.keypad(False)
        curses.echo()
        curses.endwin()

    def nap(self):
        curses.napms(int(self.screen_delay * 1000))

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
            self.display.nap()


def run_camera(camera, freq, verbose=False):
    sys.stderr.write(
        "-> celeryev: Taking snapshots with %s (every %s secs.)\n" % (
            camera, freq))
    state = State()
    cam = instantiate(camera, state, freq=freq, verbose=verbose)
    cam.install()
    conn = establish_connection()
    recv = EventReceiver(conn, handlers={"*": state.event})
    try:
        recv.capture(limit=None)
    finally:
        cam.shutter()
        conn.close()

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
        recv.capture(limit=None)
    except Exception:
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


def run_celeryev(dump=False, camera=None, frequency=1.0, verbose=False,
        **kwargs):
    if dump:
        return eventdump()
    if camera:
        return run_camera(camera, frequency, verbose=verbose)
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
