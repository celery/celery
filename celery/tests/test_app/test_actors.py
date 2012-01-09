from __future__ import absolute_import

from kombu import BrokerConnection

from celery.actors import Actor, Agent, AwareAgent

from celery.tests.utils import AppCase


class test_using_connection(AppCase):

    def assertConnection(self, cls):
        x = cls(app=self.app)
        self.assertTrue(x.connection)

        conn = BrokerConnection(transport="memory")
        x = cls(app=self.app, connection=conn)
        self.assertIs(x.connection, conn)

    def test_Actor(self):
        self.assertConnection(Actor)

    def test_Agent(self):
        self.assertConnection(Agent)

    def test_AwareAgent(self):
        self.assertConnection(AwareAgent)
