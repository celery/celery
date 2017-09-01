"""  Initialize the cherami client  """
import time

# export CLAY_CONFIG=<path to config file>
# eg. export CLAY_CONFIG=examples/cherami/configs/*
from clay import config

from tornado import ioloop

from tchannel.sync import TChannel as TChannelSyncClient
from cherami_client.client import Client

from kombu.log import get_logger


client = None
publisher = None
consumer = None

tchannel = TChannelSyncClient(name='celery_example',
                              known_peers=['127.0.0.1:4922'])
logger = get_logger('example_client')

# Initialize IOLoop here since initializing cherami client
# will need to make tchannel calls.
ioloop.IOLoop.current()

while True:
    try:
        client = Client(tchannel,
                logger,)

        publisher = client.create_publisher(
                        config.get('celery.cherami.destination'))
        publisher.open()

        consumer = client.create_consumer(
                        config.get('celery.cherami.destination'),
                        config.get('celery.cherami.consumergroup'))
        consumer.open()
        break
    except Exception as e:
        logger.exception('Failed to connect to cherami: %s', e)
        time.sleep(config.get('celery.cherami.reconnect_delay'))
