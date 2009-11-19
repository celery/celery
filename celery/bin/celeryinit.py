import sys


def main():
    from celery.loaders.default import Loader
    loader = Loader()
    conf = loader.read_configuration()
    from django.core.management import call_command, setup_environ
    sys.stderr.write("Creating database tables...\n")
    setup_environ(conf)
    call_command("syncdb")

if __name__ == "__main__":
    main()
