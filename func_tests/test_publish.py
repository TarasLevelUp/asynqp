import asynqp
import asyncio
import unittest
import gc
from asyncio import test_utils


class TestPublish(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.loop.run_until_complete(self.setUpCoroutine())

    def tearDown(self):
        # just in case if we have transport close callbacks
        self.loop.run_until_complete(self.tearDownCoroutine())
        test_utils.run_briefly(self.loop)

        self.loop.close()
        gc.collect()

    @asyncio.coroutine
    def setUpCoroutine(self):
        self.connection = yield from asynqp.connect(
            'localhost', 5672, username='guest', password='guest',
            virtual_host="asynqp-tests", loop=self.loop)
        self.channel = yield from self.connection.open_channel()
        self.exchange = yield from self.channel.declare_exchange('', 'direct')
        self.queue = yield from self.channel.declare_queue()

    @asyncio.coroutine
    def tearDownCoroutine(self):
        yield from self.queue.delete()
        yield from self.connection.close()

    def assertMessageArrived(self, expected_msg, *, timeout=0.5):
        start = self.loop.time()
        while True:
            msg = self.loop.run_until_complete(self.queue.get(no_ack=True))
            if msg is not None:
                self.assertEqual(msg.body, expected_msg)
                return
            if self.loop.time() - start < timeout:
                raise asyncio.TimeoutError()

    def test_publish(self):
        message = asynqp.Message(b"Test message")
        self.exchange.publish(message, self.queue.name)
        self.assertMessageArrived(b"Test message")
