import asynqp
import asyncio
import unittest
import gc
from asyncio import test_utils


class TestQueue(unittest.TestCase):

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

    def test_queue_get(self):
        @asyncio.coroutine
        def go():
            message = asynqp.Message(b"Test message")
            self.exchange.publish(message, self.queue.name)
            yield from asyncio.sleep(0.2, loop=self.loop)
            msg = yield from self.queue.get(no_ack=True)
            self.assertIsNotNone(msg, "Message not received")
            self.assertEqual(msg.body, b"Test message")

        self.loop.run_until_complete(go())

    def test_queue_consume(self):
        @asyncio.coroutine
        def go():
            results = []
            consumer = yield from self.queue.consume(
                results.append, no_ack=True)

            message = asynqp.Message(b"Test message")
            self.exchange.publish(message, self.queue.name)
            self.exchange.publish(message, self.queue.name)
            self.exchange.publish(message, self.queue.name)

            yield from asyncio.sleep(2, loop=self.loop)
            self.assertEqual(len(results), 3, "Not all messages received")
            self.assertEqual(results[0].body, b"Test message")
            self.assertEqual(results[1].body, b"Test message")
            self.assertEqual(results[2].body, b"Test message")

            yield from consumer.cancel()

        self.loop.run_until_complete(go())
