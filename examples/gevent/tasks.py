import requests

from celery import task


@task(ignore_result=True)
def urlopen(url):
    print(f'Opening: {url}')
    try:
        requests.get(url, timeout=10.0)
    except requests.exceptions.RequestException as exc:
        print(f'Exception for {url}: {exc!r}')
        return url, 0
    print(f'Done with: {url}')
    return url, 1
