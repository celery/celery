import os

config = os.environ.setdefault("CELERY_TEST_CONFIG_MODULE",
                               "celery.tests.config")

os.environ["CELERY_CONFIG_MODULE"] = config
os.environ["CELERY_LOADER"] = "default"


def teardown():
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
