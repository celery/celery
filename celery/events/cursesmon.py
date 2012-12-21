# -*- coding: utf-8 -*-
"""
    celery.events.cursesmon
    ~~~~~~~~~~~~~~~~~~~~~~~

    Graphical monitor of Celery events using curses.

"""
from __future__ import absolute_import
from __future__ import with_statement

import curses
import sys
import threading
import time

from datetime import datetime
from itertools import count
from textwrap import wrap
from math import ceil

from celery import VERSION_BANNER
from celery import states
from celery.app import app_or_default
from celery.utils.text import abbr, abbrtask

BORDER_SPACING = 4
LEFT_BORDER_OFFSET = 3
UUID_WIDTH = 36
STATE_WIDTH = 8
TIMESTAMP_WIDTH = 8
MIN_WORKER_WIDTH = 15
MIN_TASK_WIDTH = 16

# this module is considered experimental
# we don't care about coverage.


class CursesMonitor(object):  # pragma: no cover
    keymap = {}
    win = None
    screen_width = None
    screen_delay = 10
    selected_task = None
    selected_position = 0
    selected_str = 'Selected: '
    foreground = curses.COLOR_BLACK
    background = curses.COLOR_WHITE
    online_str = 'Workers online: '
    help_title = 'Keys: '
    help = ('j:up k:down i:info t:traceback r:result c:revoke ^c: quit')
    greet = 'celeryev %s' % VERSION_BANNER
    info_str = 'Info: '

    def __init__(self, state, keymap=None, app=None):
        self.app = app_or_default(app)
        self.keymap = keymap or self.keymap
        self.state = state
        default_keymap = {'J': self.move_selection_down,
                          'K': self.move_selection_up,
                          'C': self.revoke_selection,
                          'T': self.selection_traceback,
                          'R': self.selection_result,
                          'I': self.selection_info,
                          'L': self.selection_rate_limit}
        self.keymap = dict(default_keymap, **self.keymap)

    def format_row(self, uuid, task, worker, timestamp, state):
        mx = self.display_width

        # include spacing
        detail_width = mx - 1 - STATE_WIDTH - 1 - TIMESTAMP_WIDTH
        uuid_space = detail_width - 1 - MIN_TASK_WIDTH - 1 - MIN_WORKER_WIDTH

        if uuid_space < UUID_WIDTH:
            uuid_width = uuid_space
        else:
            uuid_width = UUID_WIDTH

        detail_width = detail_width - uuid_width - 1
        task_width = int(ceil(detail_width / 2.0))
        worker_width = detail_width - task_width - 1

        uuid = abbr(uuid, uuid_width).ljust(uuid_width)
        worker = abbr(worker, worker_width).ljust(worker_width)
        task = abbrtask(task, task_width).ljust(task_width)
        state = abbr(state, STATE_WIDTH).ljust(STATE_WIDTH)
        timestamp = timestamp.ljust(TIMESTAMP_WIDTH)

        row = '%s %s %s %s %s ' % (uuid, worker, task, timestamp, state)
        if self.screen_width is None:
            self.screen_width = len(row[:mx])
        return row[:mx]

    @property
    def screen_width(self):
        _, mx = self.win.getmaxyx()
        return mx

    @property
    def screen_height(self):
        my, _ = self.win.getmaxyx()
        return my

    @property
    def display_width(self):
        _, mx = self.win.getmaxyx()
        return mx - BORDER_SPACING

    @property
    def display_height(self):
        my, _ = self.win.getmaxyx()
        return my - 10

    @property
    def limit(self):
        return self.display_height

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

    keyalias = {curses.KEY_DOWN: 'J',
                curses.KEY_UP: 'K',
                curses.KEY_ENTER: 'I'}

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
        self.win.addstr(my - 1, 0, 'Press any key to continue...',
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
        r = 'New rate limit: '
        self.win.addstr(my - 2, 3, r, curses.A_BOLD | curses.A_UNDERLINE)
        self.win.addstr(my - 2, len(r) + 3, ' ' * (mx - len(r)))
        rlimit = self.readline(my - 2, 3 + len(r))

        if rlimit:
            reply = self.app.control.rate_limit(task.name,
                                                rlimit.strip(), reply=True)
            self.alert_remote_control_reply(reply)

    def alert_remote_control_reply(self, reply):

        def callback(my, mx, xs):
            y = count(xs).next
            if not reply:
                self.win.addstr(
                    y(), 3, 'No replies received in 1s deadline.',
                    curses.A_BOLD + curses.color_pair(2),
                )
                return

            for subreply in reply:
                curline = y()

                host, response = subreply.items()[0]
                host = '%s: ' % host
                self.win.addstr(curline, 3, host, curses.A_BOLD)
                attr = curses.A_NORMAL
                text = ''
                if 'error' in response:
                    text = response['error']
                    attr |= curses.color_pair(2)
                elif 'ok' in response:
                    text = response['ok']
                    attr |= curses.color_pair(3)
                self.win.addstr(curline, 3 + len(host), text, attr)

        return self.alert(callback, 'Remote Control Command Replies')

    def readline(self, x, y):
        buffer = str()
        curses.echo()
        try:
            i = 0
            while 1:
                ch = self.win.getch(x, y + i)
                if ch != -1:
                    if ch in (10, curses.KEY_ENTER):            # enter
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
        reply = self.app.control.revoke(self.selected_task, reply=True)
        self.alert_remote_control_reply(reply)

    def selection_info(self):
        if not self.selected_task:
            return

        def alert_callback(mx, my, xs):
            my, mx = self.win.getmaxyx()
            y = count(xs).next
            task = self.state.tasks[self.selected_task]
            info = task.info(extra=['state'])
            infoitems = [('args', info.pop('args', None)),
                         ('kwargs', info.pop('kwargs', None))] + info.items()
            for key, value in infoitems:
                if key is None:
                    continue
                value = str(value)
                curline = y()
                keys = key + ': '
                self.win.addstr(curline, 3, keys, curses.A_BOLD)
                wrapped = wrap(value, mx - 2)
                if len(wrapped) == 1:
                    self.win.addstr(
                        curline, len(keys) + 3,
                        abbr(wrapped[0],
                             self.screen_width - (len(keys) + 3)))
                else:
                    for subline in wrapped:
                        nexty = y()
                        if nexty >= my - 1:
                            subline = ' ' * 4 + '[...]'
                        elif nexty >= my:
                            break
                        self.win.addstr(
                            nexty, 3,
                            abbr(' ' * 4 + subline, self.screen_width - 4),
                            curses.A_NORMAL,
                        )

        return self.alert(
            alert_callback, 'Task details for %s' % self.selected_task,
        )

    def selection_traceback(self):
        if not self.selected_task:
            return curses.beep()
        task = self.state.tasks[self.selected_task]
        if task.state not in states.EXCEPTION_STATES:
            return curses.beep()

        def alert_callback(my, mx, xs):
            y = count(xs).next
            for line in task.traceback.split('\n'):
                self.win.addstr(y(), 3, line)

        return self.alert(
            alert_callback,
            'Task Exception Traceback for %s' % self.selected_task,
        )

    def selection_result(self):
        if not self.selected_task:
            return

        def alert_callback(my, mx, xs):
            y = count(xs).next
            task = self.state.tasks[self.selected_task]
            result = (getattr(task, 'result', None)
                      or getattr(task, 'exception', None))
            for line in wrap(result, mx - 2):
                self.win.addstr(y(), 3, line)

        return self.alert(
            alert_callback, 'Task Result for %s' % self.selected_task,
        )

    def display_task_row(self, lineno, task):
        state_color = self.state_colors.get(task.state)
        attr = curses.A_NORMAL
        if task.uuid == self.selected_task:
            attr = curses.A_STANDOUT
        timestamp = datetime.utcfromtimestamp(
            task.timestamp or time.time(),
        )
        timef = timestamp.strftime('%H:%M:%S')
        hostname = task.worker.hostname if task.worker else '*NONE*'
        line = self.format_row(task.uuid, task.name,
                               hostname,
                               timef, task.state)
        self.win.addstr(lineno, LEFT_BORDER_OFFSET, line, attr)

        if state_color:
            self.win.addstr(lineno,
                            len(line) - STATE_WIDTH + BORDER_SPACING - 1,
                            task.state, state_color | attr)

    def draw(self):
        win = self.win
        self.handle_keypress()
        x = LEFT_BORDER_OFFSET
        y = blank_line = count(2).next
        my, mx = win.getmaxyx()
        win.erase()
        win.bkgd(' ', curses.color_pair(1))
        win.border()
        win.addstr(1, x, self.greet, curses.A_DIM | curses.color_pair(5))
        blank_line()
        win.addstr(y(), x, self.format_row('UUID', 'TASK',
                                           'WORKER', 'TIME', 'STATE'),
                   curses.A_BOLD | curses.A_UNDERLINE)
        tasks = self.tasks
        if tasks:
            for row, (uuid, task) in enumerate(tasks):
                if row > self.display_height:
                    break

                if task.uuid:
                    lineno = y()
                self.display_task_row(lineno, task)

        # -- Footer
        blank_line()
        win.hline(my - 6, x, curses.ACS_HLINE, self.screen_width - 4)

        # Selected Task Info
        if self.selected_task:
            win.addstr(my - 5, x, self.selected_str, curses.A_BOLD)
            info = 'Missing extended info'
            detail = ''
            try:
                selection = self.state.tasks[self.selected_task]
            except KeyError:
                pass
            else:
                info = selection.info()
                if 'runtime' in info:
                    info['runtime'] = '%.2fs' % info['runtime']
                if 'result' in info:
                    info['result'] = abbr(info['result'], 16)
                info = ' '.join(
                    '%s=%s' % (key, value) for key, value in info.items())
                detail = '... -> key i'
            infowin = abbr(info,
                           self.screen_width - len(self.selected_str) - 2,
                           detail)
            win.addstr(my - 5, x + len(self.selected_str), infowin)
            # Make ellipsis bold
            if detail in infowin:
                detailpos = len(infowin) - len(detail)
                win.addstr(my - 5, x + len(self.selected_str) + detailpos,
                           detail, curses.A_BOLD)
        else:
            win.addstr(my - 5, x, 'No task selected', curses.A_NORMAL)

        # Workers
        if self.workers:
            win.addstr(my - 4, x, self.online_str, curses.A_BOLD)
            win.addstr(my - 4, x + len(self.online_str),
                       ', '.join(sorted(self.workers)), curses.A_NORMAL)
        else:
            win.addstr(my - 4, x, 'No workers discovered.')

        # Info
        win.addstr(my - 3, x, self.info_str, curses.A_BOLD)
        win.addstr(
            my - 3, x + len(self.info_str),
            'events:%s tasks:%s workers:%s/%s' % (
                self.state.event_count, self.state.task_count,
                len([w for w in self.state.workers.values()
                     if w.alive]),
                len(self.state.workers)),
            curses.A_DIM,
        )

        # Help
        self.safe_add_str(my - 2, x, self.help_title, curses.A_BOLD)
        self.safe_add_str(my - 2, x + len(self.help_title), self.help,
                          curses.A_DIM)
        win.refresh()

    def safe_add_str(self, y, x, string, *args, **kwargs):
        if x + len(string) > self.screen_width:
            string = string[:self.screen_width - x]
        self.win.addstr(y, x, string, *args, **kwargs)

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
        curses.napms(self.screen_delay)

    @property
    def tasks(self):
        return self.state.tasks_by_timestamp()[:self.limit]

    @property
    def workers(self):
        return [hostname for hostname, w in self.state.workers.items()
                if w.alive]


class DisplayThread(threading.Thread):  # pragma: no cover

    def __init__(self, display):
        self.display = display
        self.shutdown = False
        threading.Thread.__init__(self)

    def run(self):
        while not self.shutdown:
            self.display.draw()
            self.display.nap()


def capture_events(app, state, display):  # pragma: no cover

    def on_connection_error(exc, interval):
        sys.stderr.write('Connection Error: %r. Retry in %ss.' % (
            exc, interval))

    while 1:
        sys.stderr.write('-> evtop: starting capture...\n')
        with app.connection() as conn:
            try:
                conn.ensure_connection(on_connection_error,
                                       app.conf.BROKER_CONNECTION_MAX_RETRIES)
                recv = app.events.Receiver(conn, handlers={'*': state.event})
                display.resetscreen()
                display.init_screen()
                with recv.consumer():
                    recv.drain_events(timeout=1, ignore_timeouts=True)
            except (conn.connection_errors, conn.channel_errors), exc:
                sys.stderr.write('Connection lost: %r' % (exc, ))


def evtop(app=None):  # pragma: no cover
    app = app_or_default(app)
    state = app.events.State()
    display = CursesMonitor(state, app=app)
    display.init_screen()
    refresher = DisplayThread(display)
    refresher.start()
    try:
        capture_events(app, state, display)
    except Exception:
        refresher.shutdown = True
        refresher.join()
        display.resetscreen()
        raise
    except (KeyboardInterrupt, SystemExit):
        refresher.shutdown = True
        refresher.join()
        display.resetscreen()


if __name__ == '__main__':  # pragma: no cover
    evtop()
