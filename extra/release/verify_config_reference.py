from __future__ import print_function

from fileinput import input as _input
from sys import exit, stderr

from celery.app.defaults import NAMESPACES, flatten

ignore = frozenset([
    'CELERYD_AGENT',
    'CELERYD_POOL_PUTLOCKS',
    'BROKER_HOST',
    'BROKER_USER',
    'BROKER_PASSWORD',
    'BROKER_VHOST',
    'BROKER_PORT',
    'CELERY_REDIS_HOST',
    'CELERY_REDIS_PORT',
    'CELERY_REDIS_DB',
    'CELERY_REDIS_PASSWORD',
    'CELERYD_FORCE_EXECV',
])


def is_ignored(setting, option):
    return setting in ignore or option.deprecate_by


def find_undocumented_settings(directive='.. setting:: '):
    settings = dict(flatten(NAMESPACES))
    all = set(settings)
    documented = set(
        line.strip()[len(directive):].strip() for line in _input()
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
