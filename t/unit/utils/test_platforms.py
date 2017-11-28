from __future__ import absolute_import, unicode_literals

import errno
import os
import signal
import sys
import tempfile

import pytest
from case import Mock, call, mock, patch, skip

from celery import _find_option_with_arg, platforms
from celery.exceptions import SecurityError
from celery.five import WhateverIO
from celery.platforms import (DaemonContext, LockFailed, Pidfile,
                              _setgroups_hack, check_privileges,
                              close_open_fds, create_pidlock, detached,
                              fd_by_path, get_fdmax, ignore_errno, initgroups,
                              isatty, maybe_drop_privileges, parse_gid,
                              parse_uid, set_mp_process_title,
                              set_process_title, setgid, setgroups, setuid,
                              signals)

try:
    import resource
except ImportError:  # pragma: no cover
    resource = None  # noqa


def test_isatty():
    fh = Mock(name='fh')
    assert isatty(fh) is fh.isatty()
    fh.isatty.side_effect = AttributeError()
    assert not isatty(fh)


class test_find_option_with_arg:

    def test_long_opt(self):
        assert _find_option_with_arg(
            ['--foo=bar'], long_opts=['--foo']) == 'bar'

    def test_short_opt(self):
        assert _find_option_with_arg(
            ['-f', 'bar'], short_opts=['-f']) == 'bar'


@skip.if_win32()
def test_fd_by_path():
    test_file = tempfile.NamedTemporaryFile()
    try:
        keep = fd_by_path([test_file.name])
        assert keep == [test_file.file.fileno()]
        with patch('os.open') as _open:
            _open.side_effect = OSError()
            assert not fd_by_path([test_file.name])
    finally:
        test_file.close()


def test_close_open_fds(patching):
    _close = patching('os.close')
    fdmax = patching('celery.platforms.get_fdmax')
    with patch('os.closerange', create=True) as closerange:
        fdmax.return_value = 3
        close_open_fds()
        if not closerange.called:
            _close.assert_has_calls([call(2), call(1), call(0)])
            _close.side_effect = OSError()
            _close.side_effect.errno = errno.EBADF
        close_open_fds()


class test_ignore_errno:

    def test_raises_EBADF(self):
        with ignore_errno('EBADF'):
            exc = OSError()
            exc.errno = errno.EBADF
            raise exc

    def test_otherwise(self):
        with pytest.raises(OSError):
            with ignore_errno('EBADF'):
                exc = OSError()
                exc.errno = errno.ENOENT
                raise exc


class test_set_process_title:

    def test_no_setps(self):
        prev, platforms._setproctitle = platforms._setproctitle, None
        try:
            set_process_title('foo')
        finally:
            platforms._setproctitle = prev

    @patch('celery.platforms.set_process_title')
    @patch('celery.platforms.current_process')
    def test_mp_no_hostname(self, current_process, set_process_title):
        current_process().name = 'Foo'
        set_mp_process_title('foo', info='hello')
        set_process_title.assert_called_with('foo:Foo', info='hello')

    @patch('celery.platforms.set_process_title')
    @patch('celery.platforms.current_process')
    def test_mp_hostname(self, current_process, set_process_title):
        current_process().name = 'Foo'
        set_mp_process_title('foo', hostname='a@q.com', info='hello')
        set_process_title.assert_called_with('foo: a@q.com:Foo', info='hello')


class test_Signals:

    @patch('signal.getsignal')
    def test_getitem(self, getsignal):
        signals['SIGINT']
        getsignal.assert_called_with(signal.SIGINT)

    def test_supported(self):
        assert signals.supported('INT')
        assert not signals.supported('SIGIMAGINARY')

    @skip.if_win32()
    def test_reset_alarm(self):
        with patch('signal.alarm') as _alarm:
            signals.reset_alarm()
            _alarm.assert_called_with(0)

    def test_arm_alarm(self):
        if hasattr(signal, 'setitimer'):
            with patch('signal.setitimer', create=True) as seti:
                signals.arm_alarm(30)
                seti.assert_called()

    def test_signum(self):
        assert signals.signum(13) == 13
        assert signals.signum('INT') == signal.SIGINT
        assert signals.signum('SIGINT') == signal.SIGINT
        with pytest.raises(TypeError):
            signals.signum('int')
            signals.signum(object())

    @patch('signal.signal')
    def test_ignore(self, set):
        signals.ignore('SIGINT')
        set.assert_called_with(signals.signum('INT'), signals.ignored)
        signals.ignore('SIGTERM')
        set.assert_called_with(signals.signum('TERM'), signals.ignored)

    @patch('signal.signal')
    def test_reset(self, set):
        signals.reset('SIGINT')
        set.assert_called_with(signals.signum('INT'), signals.default)

    @patch('signal.signal')
    def test_setitem(self, set):
        def handle(*args):
            return args
        signals['INT'] = handle
        set.assert_called_with(signal.SIGINT, handle)

    @patch('signal.signal')
    def test_setitem_raises(self, set):
        set.side_effect = ValueError()
        signals['INT'] = lambda *a: a


@skip.if_win32()
class test_get_fdmax:

    @patch('resource.getrlimit')
    def test_when_infinity(self, getrlimit):
        with patch('os.sysconf') as sysconfig:
            sysconfig.side_effect = KeyError()
            getrlimit.return_value = [None, resource.RLIM_INFINITY]
            default = object()
            assert get_fdmax(default) is default

    @patch('resource.getrlimit')
    def test_when_actual(self, getrlimit):
        with patch('os.sysconf') as sysconfig:
            sysconfig.side_effect = KeyError()
            getrlimit.return_value = [None, 13]
            assert get_fdmax(None) == 13


@skip.if_win32()
class test_maybe_drop_privileges:

    def test_on_windows(self):
        prev, sys.platform = sys.platform, 'win32'
        try:
            maybe_drop_privileges()
        finally:
            sys.platform = prev

    @patch('os.getegid')
    @patch('os.getgid')
    @patch('os.geteuid')
    @patch('os.getuid')
    @patch('celery.platforms.parse_uid')
    @patch('celery.platforms.parse_gid')
    @patch('pwd.getpwuid')
    @patch('celery.platforms.setgid')
    @patch('celery.platforms.setuid')
    @patch('celery.platforms.initgroups')
    def test_with_uid(self, initgroups, setuid, setgid,
                      getpwuid, parse_gid, parse_uid, getuid, geteuid,
                      getgid, getegid):
        geteuid.return_value = 10
        getuid.return_value = 10

        class pw_struct(object):
            pw_gid = 50001

        def raise_on_second_call(*args, **kwargs):
            setuid.side_effect = OSError()
            setuid.side_effect.errno = errno.EPERM
        setuid.side_effect = raise_on_second_call
        getpwuid.return_value = pw_struct()
        parse_uid.return_value = 5001
        parse_gid.return_value = 5001
        maybe_drop_privileges(uid='user')
        parse_uid.assert_called_with('user')
        getpwuid.assert_called_with(5001)
        setgid.assert_called_with(50001)
        initgroups.assert_called_with(5001, 50001)
        setuid.assert_has_calls([call(5001), call(0)])

        setuid.side_effect = raise_on_second_call

        def to_root_on_second_call(mock, first):
            return_value = [first]

            def on_first_call(*args, **kwargs):
                ret, return_value[0] = return_value[0], 0
                return ret
            mock.side_effect = on_first_call
        to_root_on_second_call(geteuid, 10)
        to_root_on_second_call(getuid, 10)
        with pytest.raises(SecurityError):
            maybe_drop_privileges(uid='user')

        getuid.return_value = getuid.side_effect = None
        geteuid.return_value = geteuid.side_effect = None
        getegid.return_value = 0
        getgid.return_value = 0
        setuid.side_effect = raise_on_second_call
        with pytest.raises(SecurityError):
            maybe_drop_privileges(gid='group')

        getuid.reset_mock()
        geteuid.reset_mock()
        setuid.reset_mock()
        getuid.side_effect = geteuid.side_effect = None

        def raise_on_second_call(*args, **kwargs):
            setuid.side_effect = OSError()
            setuid.side_effect.errno = errno.ENOENT
        setuid.side_effect = raise_on_second_call
        with pytest.raises(OSError):
            maybe_drop_privileges(uid='user')

    @patch('celery.platforms.parse_uid')
    @patch('celery.platforms.parse_gid')
    @patch('celery.platforms.setgid')
    @patch('celery.platforms.setuid')
    @patch('celery.platforms.initgroups')
    def test_with_guid(self, initgroups, setuid, setgid,
                       parse_gid, parse_uid):

        def raise_on_second_call(*args, **kwargs):
            setuid.side_effect = OSError()
            setuid.side_effect.errno = errno.EPERM
        setuid.side_effect = raise_on_second_call
        parse_uid.return_value = 5001
        parse_gid.return_value = 50001
        maybe_drop_privileges(uid='user', gid='group')
        parse_uid.assert_called_with('user')
        parse_gid.assert_called_with('group')
        setgid.assert_called_with(50001)
        initgroups.assert_called_with(5001, 50001)
        setuid.assert_has_calls([call(5001), call(0)])

        setuid.side_effect = None
        with pytest.raises(SecurityError):
            maybe_drop_privileges(uid='user', gid='group')
        setuid.side_effect = OSError()
        setuid.side_effect.errno = errno.EINVAL
        with pytest.raises(OSError):
            maybe_drop_privileges(uid='user', gid='group')

    @patch('celery.platforms.setuid')
    @patch('celery.platforms.setgid')
    @patch('celery.platforms.parse_gid')
    def test_only_gid(self, parse_gid, setgid, setuid):
        parse_gid.return_value = 50001
        maybe_drop_privileges(gid='group')
        parse_gid.assert_called_with('group')
        setgid.assert_called_with(50001)
        setuid.assert_not_called()


@skip.if_win32()
class test_setget_uid_gid:

    @patch('celery.platforms.parse_uid')
    @patch('os.setuid')
    def test_setuid(self, _setuid, parse_uid):
        parse_uid.return_value = 5001
        setuid('user')
        parse_uid.assert_called_with('user')
        _setuid.assert_called_with(5001)

    @patch('celery.platforms.parse_gid')
    @patch('os.setgid')
    def test_setgid(self, _setgid, parse_gid):
        parse_gid.return_value = 50001
        setgid('group')
        parse_gid.assert_called_with('group')
        _setgid.assert_called_with(50001)

    def test_parse_uid_when_int(self):
        assert parse_uid(5001) == 5001

    @patch('pwd.getpwnam')
    def test_parse_uid_when_existing_name(self, getpwnam):

        class pwent(object):
            pw_uid = 5001

        getpwnam.return_value = pwent()
        assert parse_uid('user') == 5001

    @patch('pwd.getpwnam')
    def test_parse_uid_when_nonexisting_name(self, getpwnam):
        getpwnam.side_effect = KeyError('user')

        with pytest.raises(KeyError):
            parse_uid('user')

    def test_parse_gid_when_int(self):
        assert parse_gid(50001) == 50001

    @patch('grp.getgrnam')
    def test_parse_gid_when_existing_name(self, getgrnam):

        class grent(object):
            gr_gid = 50001

        getgrnam.return_value = grent()
        assert parse_gid('group') == 50001

    @patch('grp.getgrnam')
    def test_parse_gid_when_nonexisting_name(self, getgrnam):
        getgrnam.side_effect = KeyError('group')
        with pytest.raises(KeyError):
            parse_gid('group')


@skip.if_win32()
class test_initgroups:

    @patch('pwd.getpwuid')
    @patch('os.initgroups', create=True)
    def test_with_initgroups(self, initgroups_, getpwuid):
        getpwuid.return_value = ['user']
        initgroups(5001, 50001)
        initgroups_.assert_called_with('user', 50001)

    @patch('celery.platforms.setgroups')
    @patch('grp.getgrall')
    @patch('pwd.getpwuid')
    def test_without_initgroups(self, getpwuid, getgrall, setgroups):
        prev = getattr(os, 'initgroups', None)
        try:
            delattr(os, 'initgroups')
        except AttributeError:
            pass
        try:
            getpwuid.return_value = ['user']

            class grent(object):
                gr_mem = ['user']

                def __init__(self, gid):
                    self.gr_gid = gid

            getgrall.return_value = [grent(1), grent(2), grent(3)]
            initgroups(5001, 50001)
            setgroups.assert_called_with([1, 2, 3])
        finally:
            if prev:
                os.initgroups = prev


@skip.if_win32()
class test_detached:

    def test_without_resource(self):
        prev, platforms.resource = platforms.resource, None
        try:
            with pytest.raises(RuntimeError):
                detached()
        finally:
            platforms.resource = prev

    @patch('celery.platforms._create_pidlock')
    @patch('celery.platforms.signals')
    @patch('celery.platforms.maybe_drop_privileges')
    @patch('os.geteuid')
    @patch(mock.open_fqdn)
    def test_default(self, open, geteuid, maybe_drop,
                     signals, pidlock):
        geteuid.return_value = 0
        context = detached(uid='user', gid='group')
        assert isinstance(context, DaemonContext)
        signals.reset.assert_called_with('SIGCLD')
        maybe_drop.assert_called_with(uid='user', gid='group')
        open.return_value = Mock()

        geteuid.return_value = 5001
        context = detached(uid='user', gid='group', logfile='/foo/bar')
        assert isinstance(context, DaemonContext)
        assert context.after_chdir
        context.after_chdir()
        open.assert_called_with('/foo/bar', 'a')
        open.return_value.close.assert_called_with()

        context = detached(pidfile='/foo/bar/pid')
        assert isinstance(context, DaemonContext)
        assert context.after_chdir
        context.after_chdir()
        pidlock.assert_called_with('/foo/bar/pid')


@skip.if_win32()
class test_DaemonContext:

    @patch('multiprocessing.util._run_after_forkers')
    @patch('os.fork')
    @patch('os.setsid')
    @patch('os._exit')
    @patch('os.chdir')
    @patch('os.umask')
    @patch('os.close')
    @patch('os.closerange')
    @patch('os.open')
    @patch('os.dup2')
    @patch('celery.platforms.close_open_fds')
    def test_open(self, _close_fds, dup2, open, close, closer, umask, chdir,
                  _exit, setsid, fork, run_after_forkers):
        x = DaemonContext(workdir='/opt/workdir', umask=0o22)
        x.stdfds = [0, 1, 2]

        fork.return_value = 0
        with x:
            assert x._is_open
            with x:
                pass
        assert fork.call_count == 2
        setsid.assert_called_with()
        _exit.assert_not_called()

        chdir.assert_called_with(x.workdir)
        umask.assert_called_with(0o22)
        dup2.assert_called()

        fork.reset_mock()
        fork.return_value = 1
        x = DaemonContext(workdir='/opt/workdir')
        x.stdfds = [0, 1, 2]
        with x:
            pass
        assert fork.call_count == 1
        _exit.assert_called_with(0)

        x = DaemonContext(workdir='/opt/workdir', fake=True)
        x.stdfds = [0, 1, 2]
        x._detach = Mock()
        with x:
            pass
        x._detach.assert_not_called()

        x.after_chdir = Mock()
        with x:
            pass
        x.after_chdir.assert_called_with()

        x = DaemonContext(workdir='/opt/workdir', umask='0755')
        assert x.umask == 493
        x = DaemonContext(workdir='/opt/workdir', umask='493')
        assert x.umask == 493

        x.redirect_to_null(None)

        with patch('celery.platforms.mputil') as mputil:
            x = DaemonContext(after_forkers=True)
            x.open()
            mputil._run_after_forkers.assert_called_with()
            x = DaemonContext(after_forkers=False)
            x.open()


@skip.if_win32()
class test_Pidfile:

    @patch('celery.platforms.Pidfile')
    def test_create_pidlock(self, Pidfile):
        p = Pidfile.return_value = Mock()
        p.is_locked.return_value = True
        p.remove_if_stale.return_value = False
        with mock.stdouts() as (_, err):
            with pytest.raises(SystemExit):
                create_pidlock('/var/pid')
            assert 'already exists' in err.getvalue()

        p.remove_if_stale.return_value = True
        ret = create_pidlock('/var/pid')
        assert ret is p

    def test_context(self):
        p = Pidfile('/var/pid')
        p.write_pid = Mock()
        p.remove = Mock()

        with p as _p:
            assert _p is p
        p.write_pid.assert_called_with()
        p.remove.assert_called_with()

    def test_acquire_raises_LockFailed(self):
        p = Pidfile('/var/pid')
        p.write_pid = Mock()
        p.write_pid.side_effect = OSError()

        with pytest.raises(LockFailed):
            with p:
                pass

    @patch('os.path.exists')
    def test_is_locked(self, exists):
        p = Pidfile('/var/pid')
        exists.return_value = True
        assert p.is_locked()
        exists.return_value = False
        assert not p.is_locked()

    def test_read_pid(self):
        with mock.open() as s:
            s.write('1816\n')
            s.seek(0)
            p = Pidfile('/var/pid')
            assert p.read_pid() == 1816

    def test_read_pid_partially_written(self):
        with mock.open() as s:
            s.write('1816')
            s.seek(0)
            p = Pidfile('/var/pid')
            with pytest.raises(ValueError):
                p.read_pid()

    def test_read_pid_raises_ENOENT(self):
        exc = IOError()
        exc.errno = errno.ENOENT
        with mock.open(side_effect=exc):
            p = Pidfile('/var/pid')
            assert p.read_pid() is None

    def test_read_pid_raises_IOError(self):
        exc = IOError()
        exc.errno = errno.EAGAIN
        with mock.open(side_effect=exc):
            p = Pidfile('/var/pid')
            with pytest.raises(IOError):
                p.read_pid()

    def test_read_pid_bogus_pidfile(self):
        with mock.open() as s:
            s.write('eighteensixteen\n')
            s.seek(0)
            p = Pidfile('/var/pid')
            with pytest.raises(ValueError):
                p.read_pid()

    @patch('os.unlink')
    def test_remove(self, unlink):
        unlink.return_value = True
        p = Pidfile('/var/pid')
        p.remove()
        unlink.assert_called_with(p.path)

    @patch('os.unlink')
    def test_remove_ENOENT(self, unlink):
        exc = OSError()
        exc.errno = errno.ENOENT
        unlink.side_effect = exc
        p = Pidfile('/var/pid')
        p.remove()
        unlink.assert_called_with(p.path)

    @patch('os.unlink')
    def test_remove_EACCES(self, unlink):
        exc = OSError()
        exc.errno = errno.EACCES
        unlink.side_effect = exc
        p = Pidfile('/var/pid')
        p.remove()
        unlink.assert_called_with(p.path)

    @patch('os.unlink')
    def test_remove_OSError(self, unlink):
        exc = OSError()
        exc.errno = errno.EAGAIN
        unlink.side_effect = exc
        p = Pidfile('/var/pid')
        with pytest.raises(OSError):
            p.remove()
        unlink.assert_called_with(p.path)

    @patch('os.kill')
    def test_remove_if_stale_process_alive(self, kill):
        p = Pidfile('/var/pid')
        p.read_pid = Mock()
        p.read_pid.return_value = 1816
        kill.return_value = 0
        assert not p.remove_if_stale()
        kill.assert_called_with(1816, 0)
        p.read_pid.assert_called_with()

        kill.side_effect = OSError()
        kill.side_effect.errno = errno.ENOENT
        assert not p.remove_if_stale()

    @patch('os.kill')
    def test_remove_if_stale_process_dead(self, kill):
        with mock.stdouts():
            p = Pidfile('/var/pid')
            p.read_pid = Mock()
            p.read_pid.return_value = 1816
            p.remove = Mock()
            exc = OSError()
            exc.errno = errno.ESRCH
            kill.side_effect = exc
            assert p.remove_if_stale()
            kill.assert_called_with(1816, 0)
            p.remove.assert_called_with()

    def test_remove_if_stale_broken_pid(self):
        with mock.stdouts():
            p = Pidfile('/var/pid')
            p.read_pid = Mock()
            p.read_pid.side_effect = ValueError()
            p.remove = Mock()

            assert p.remove_if_stale()
            p.remove.assert_called_with()

    def test_remove_if_stale_no_pidfile(self):
        p = Pidfile('/var/pid')
        p.read_pid = Mock()
        p.read_pid.return_value = None
        p.remove = Mock()

        assert p.remove_if_stale()
        p.remove.assert_called_with()

    @patch('os.fsync')
    @patch('os.getpid')
    @patch('os.open')
    @patch('os.fdopen')
    @patch(mock.open_fqdn)
    def test_write_pid(self, open_, fdopen, osopen, getpid, fsync):
        getpid.return_value = 1816
        osopen.return_value = 13
        w = fdopen.return_value = WhateverIO()
        w.close = Mock()
        r = open_.return_value = WhateverIO()
        r.write('1816\n')
        r.seek(0)

        p = Pidfile('/var/pid')
        p.write_pid()
        w.seek(0)
        assert w.readline() == '1816\n'
        w.close.assert_called()
        getpid.assert_called_with()
        osopen.assert_called_with(
            p.path, platforms.PIDFILE_FLAGS, platforms.PIDFILE_MODE,
        )
        fdopen.assert_called_with(13, 'w')
        fsync.assert_called_with(13)
        open_.assert_called_with(p.path)

    @patch('os.fsync')
    @patch('os.getpid')
    @patch('os.open')
    @patch('os.fdopen')
    @patch(mock.open_fqdn)
    def test_write_reread_fails(self, open_, fdopen,
                                osopen, getpid, fsync):
        getpid.return_value = 1816
        osopen.return_value = 13
        w = fdopen.return_value = WhateverIO()
        w.close = Mock()
        r = open_.return_value = WhateverIO()
        r.write('11816\n')
        r.seek(0)

        p = Pidfile('/var/pid')
        with pytest.raises(LockFailed):
            p.write_pid()


class test_setgroups:

    @patch('os.setgroups', create=True)
    def test_setgroups_hack_ValueError(self, setgroups):

        def on_setgroups(groups):
            if len(groups) <= 200:
                setgroups.return_value = True
                return
            raise ValueError()
        setgroups.side_effect = on_setgroups
        _setgroups_hack(list(range(400)))

        setgroups.side_effect = ValueError()
        with pytest.raises(ValueError):
            _setgroups_hack(list(range(400)))

    @patch('os.setgroups', create=True)
    def test_setgroups_hack_OSError(self, setgroups):
        exc = OSError()
        exc.errno = errno.EINVAL

        def on_setgroups(groups):
            if len(groups) <= 200:
                setgroups.return_value = True
                return
            raise exc
        setgroups.side_effect = on_setgroups

        _setgroups_hack(list(range(400)))

        setgroups.side_effect = exc
        with pytest.raises(OSError):
            _setgroups_hack(list(range(400)))

        exc2 = OSError()
        exc.errno = errno.ESRCH
        setgroups.side_effect = exc2
        with pytest.raises(OSError):
            _setgroups_hack(list(range(400)))

    @skip.if_win32()
    @patch('celery.platforms._setgroups_hack')
    def test_setgroups(self, hack):
        with patch('os.sysconf') as sysconf:
            sysconf.return_value = 100
            setgroups(list(range(400)))
            hack.assert_called_with(list(range(100)))

    @skip.if_win32()
    @patch('celery.platforms._setgroups_hack')
    def test_setgroups_sysconf_raises(self, hack):
        with patch('os.sysconf') as sysconf:
            sysconf.side_effect = ValueError()
            setgroups(list(range(400)))
            hack.assert_called_with(list(range(400)))

    @skip.if_win32()
    @patch('os.getgroups')
    @patch('celery.platforms._setgroups_hack')
    def test_setgroups_raises_ESRCH(self, hack, getgroups):
        with patch('os.sysconf') as sysconf:
            sysconf.side_effect = ValueError()
            esrch = OSError()
            esrch.errno = errno.ESRCH
            hack.side_effect = esrch
            with pytest.raises(OSError):
                setgroups(list(range(400)))

    @skip.if_win32()
    @patch('os.getgroups')
    @patch('celery.platforms._setgroups_hack')
    def test_setgroups_raises_EPERM(self, hack, getgroups):
        with patch('os.sysconf') as sysconf:
            sysconf.side_effect = ValueError()
            eperm = OSError()
            eperm.errno = errno.EPERM
            hack.side_effect = eperm
            getgroups.return_value = list(range(400))
            setgroups(list(range(400)))
            getgroups.assert_called_with()

            getgroups.return_value = [1000]
            with pytest.raises(OSError):
                setgroups(list(range(400)))
            getgroups.assert_called_with()


def test_check_privileges():
    class Obj(object):
        fchown = 13
    prev, platforms.os = platforms.os, Obj()
    try:
        with pytest.raises(SecurityError):
            check_privileges({'pickle'})
    finally:
        platforms.os = prev
    prev, platforms.os = platforms.os, object()
    try:
        check_privileges({'pickle'})
    finally:
        platforms.os = prev
