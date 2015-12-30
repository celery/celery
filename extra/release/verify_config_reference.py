from __future__ import print_function, unicode_literals

from fileinput import input as _input
from sys import exit, stderr

from celery.app.defaults import NAMESPACES, flatten

ignore = {
    'worker_agent',
    'worker_pool_putlocks',
    'broker_host',
    'broker_user',
    'broker_password',
    'broker_vhost',
    'broker_port',
    'broker_transport',
    'chord_propagates',
    'redis_host',
    'redis_port',
    'redis_db',
    'redis_password',
    'worker_force_execv',
}


def is_ignored(setting, option):
    return setting in ignore or option.deprecate_by


def find_undocumented_settings(directive='.. setting:: '):
    settings = dict(flatten(NAMESPACES))
    all = set(settings)
    inp = (l.decode('utf-8') for l in _input())
    documented = set(
        line.strip()[len(directive):].strip() for line in inp
        if line.strip().startswith(directive)
    )
    return [setting for setting in all ^ documented
            if not is_ignored(setting, settings[setting])]


if __name__ == '__main__':
    sep = '\n  * '
    missing = find_undocumented_settings()
    if missing:
        print(
            'Error: found undocumented settings:{0}{1}'.format(
                sep, sep.join(sorted(missing))),
            file=stderr,
        )
        exit(1)
    print('OK: Configuration reference complete :-)')
    exit(0)
