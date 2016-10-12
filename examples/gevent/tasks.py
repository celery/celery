from __future__ import absolute_import, unicode_literals

import requests

from celery import task


@task(ignore_result=True)
def urlopen(url):
    print('Opening: {0}'.format(url))
    try:
        requests.get(url)
    except requests.exceptions.RequestException as exc:
        print('Exception for {0}: {1!r}'.format(url, exc))
        return url, 0
    print('Done with: {0}'.format(url))
    return url, 1
