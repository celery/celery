"""  Initialize the cherami client  """
import ujson, time

from clay import config

from tornado import ioloop

from tchannel.sync import TChannel as TChannelSyncClient
from cherami_client.client import Client


client = None
publisher = None
consumer = None

tchannel = TChannelSyncClient(name='celery_example', known_peers=['127.0.0.1:4922'])
logger = get_logger('example_client')

# call this to prevent celery initialization from stalling when opening the consumer/publisher
ioloop.IOLoop.current()

while True:
    try:
        client = Client(tchannel,
                        logger,
                        deployment_str=config.get('celery.cherami.deployment_str'),
                        reconfigure_interval_seconds=config.get('celery.cherami.reconfigure_interval_seconds'))

        publisher = client.create_publisher(config.get('celery.cherami.destination'))
        publisher.open()

        consumer = client.create_consumer(config.get('celery.cherami.destination'),
                                          config.get('celery.cherami.consumergroup'))
        consumer.open()
        break
    except Exception as e:
        logger.exception('Failed to connect to cherami: %s', e)
        time.sleep(config.get('celery.cherami.reconnect_delay'))
