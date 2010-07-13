import os

config = os.environ.setdefault("CELERY_FUNTEST_CONFIG_MODULE",
                               "celery.tests.functional.config")

os.environ["CELERY_CONFIG_MODULE"] = config
os.environ["CELERY_LOADER"] = "default"
