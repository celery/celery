from fileinput import input
from sys import exit, stderr

from celery.app.defaults import NAMESPACES, flatten

ignore = frozenset([
    "BROKER_INSIST",
    "CELERYD_POOL_PUTLOCKS",
    "CELERY_AMQP_TASK_RESULT_CONNECTION_MAX",
    "BROKER_HOST",
    "BROKER_USER",
    "BROKER_PASSWORD",
    "BROKER_VHOST",
    "BROKER_PORT",
    "CELERY_REDIS_HOST",
    "CELERY_REDIS_PORT",
    "CELERY_REDIS_DB",
    "CELERY_REDIS_PASSWORD",
])


def is_ignored(setting, option):
    return setting in ignore or option.deprecate_by


def find_undocumented_settings(directive=".. setting:: "):
    settings = dict(flatten(NAMESPACES))
    all = set(settings)
    documented = set(line.strip()[len(directive):].strip()
                        for line in input()
                            if line.strip().startswith(directive))
    return [setting for setting in all ^ documented
                if not is_ignored(setting, settings[setting])]


if __name__ == "__main__":
    sep = """\n  * """
    missing = find_undocumented_settings()
    if missing:
        stderr.write("Error: found undocumented settings:%s%s\n" % (
                        sep, sep.join(sorted(missing))))
        exit(1)
    print("OK: Configuration reference complete :-)")
    exit(0)
