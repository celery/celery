from __future__ import absolute_import, print_function, unicode_literals

import requests

from celery import task


@task(ignore_result=True)
def urlopen(url):
    print('Opening: {}'.format(url))
    try:
        requests.get(url)
    except requests.exceptions.RequestException as exc:
        print('Exception for {}: {!r}'.format(url, exc))
        return url, 0
    print('Done with: {}'.format(url))
    return url, 1
