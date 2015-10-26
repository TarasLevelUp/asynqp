import asynqp
import asyncio
import unittest
import gc
from unittest import mock
from asyncio import test_utils
from asynqp.exceptions import ConsumerCancelled, ConnectionLostError, \
    ClientConnectionClosed


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

    def _run_test(self, coro):
        self.loop.run_until_complete(
            asyncio.wait_for(coro, timeout=10, loop=self.loop))

    @asyncio.coroutine
    def setUpCoroutine(self):
        self.connection = yield from asynqp.connect(
            'localhost', 5672, username='guest', password='guest',
            virtual_host="asynqp-tests", loop=self.loop)
        self.channel = yield from self.connection.open_channel()
        self.exchange = yield from self.channel.declare_exchange('', 'direct')
        self.queue = yield from self.channel.declare_queue(
            auto_delete=True, exclusive=True)

    @asyncio.coroutine
    def tearDownCoroutine(self):
        yield from asyncio.wait_for(
            self.connection.close(), timeout=3, loop=self.loop)

    def test_queue_get(self):
        @asyncio.coroutine
        def go():
            message = asynqp.Message(b"Test message")
            self.exchange.publish(message, self.queue.name)
            yield from asyncio.sleep(0.2, loop=self.loop)
            msg = yield from self.queue.get(no_ack=True)
            self.assertIsNotNone(msg, "Message not received")
            self.assertEqual(msg.body, b"Test message")

        self._run_test(go())

    def test_queue_consume_callback(self):
        @asyncio.coroutine
        def go():
            results = []
            consumer = yield from self.queue.consume(
                results.append, no_ack=True)

            message = asynqp.Message(b"Test message")
            self.exchange.publish(message, self.queue.name)
            self.exchange.publish(message, self.queue.name)
            self.exchange.publish(message, self.queue.name)

            for i in range(20):
                if len(results) == 3:
                    break
                yield from asyncio.sleep(0.1, loop=self.loop)
            self.assertEqual(len(results), 3, "Not all messages received")
            self.assertEqual(results[0].body, b"Test message")
            self.assertEqual(results[1].body, b"Test message")
            self.assertEqual(results[2].body, b"Test message")

            yield from consumer.cancel()

        self._run_test(go())

    def test_queue_consume_queue(self):
        @asyncio.coroutine
        def go():
            results = []
            consumer = yield from self.queue.queued_consumer(no_ack=True)

            message = asynqp.Message(b"Test message1")
            self.exchange.publish(message, self.queue.name)
            message = asynqp.Message(b"Test message2")
            self.exchange.publish(message, self.queue.name)
            message = asynqp.Message(b"Test message3")
            self.exchange.publish(message, self.queue.name)

            for i in range(3):
                x = yield from consumer.get()
                results.append(x)
            self.assertEqual(len(results), 3, "Not all messages received")
            self.assertEqual(results[0].body, b"Test message1")
            self.assertEqual(results[1].body, b"Test message2")
            self.assertEqual(results[2].body, b"Test message3")

            # Any more calls will lock queue until we received a message
            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(
                    consumer.get(), timeout=0.1, loop=self.loop)

            message = asynqp.Message(b"Test message4")
            self.exchange.publish(message, self.queue.name)

            for i in range(20):
                if not consumer.empty():
                    break
                yield from asyncio.sleep(0.1, loop=self.loop)

            yield from consumer.cancel()

            # If messages still present - return them
            msg = yield from consumer.get()
            self.assertEqual(msg.body, b"Test message4")

            # Any call after will raise errors
            with self.assertRaises(ConsumerCancelled):
                yield from asyncio.wait_for(
                    consumer.get(), loop=self.loop, timeout=0.01)

        self._run_test(go())

    def test_queue_consume_many_queue(self):
        @asyncio.coroutine
        def go():
            results = []
            consumer = yield from self.queue.queued_consumer(no_ack=True)

            message = asynqp.Message(b"Test message1")
            self.exchange.publish(message, self.queue.name)
            message = asynqp.Message(b"Test message2")
            self.exchange.publish(message, self.queue.name)
            message = asynqp.Message(b"Test message3")
            self.exchange.publish(message, self.queue.name)

            yield from asyncio.sleep(0.1, loop=self.loop)
            results = yield from consumer.getmany()
            self.assertEqual(len(results), 3, "Not all messages received")
            self.assertEqual(results[0].body, b"Test message1")
            self.assertEqual(results[1].body, b"Test message2")
            self.assertEqual(results[2].body, b"Test message3")

            # Any more calls will lock queue until we received a message
            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(
                    consumer.getmany(), timeout=0.1, loop=self.loop)

            message = asynqp.Message(b"Test message4")
            self.exchange.publish(message, self.queue.name)

            for i in range(20):
                if not consumer.empty():
                    break
                yield from asyncio.sleep(0.1, loop=self.loop)

            yield from consumer.cancel()

            # If messages still present - return them
            [msg] = yield from consumer.getmany()
            self.assertEqual(msg.body, b"Test message4")

            # Any call after will raise errors
            with self.assertRaises(ConsumerCancelled):
                yield from asyncio.wait_for(
                    consumer.getmany(), loop=self.loop, timeout=0.01)

        self._run_test(go())

    def test_queue_disconnect_client_consumer_no_ack(self):
        @asyncio.coroutine
        def go():
            consumer = yield from self.queue.queued_consumer(no_ack=True)

            message = asynqp.Message(b"Test message1")
            self.exchange.publish(message, self.queue.name)

            msg = yield from consumer.get()
            self.assertEqual(msg.body, b"Test message1")

            message = asynqp.Message(b"Test message before disconnect")
            self.exchange.publish(message, self.queue.name)

            for i in range(20):
                if not consumer.empty():
                    break
                yield from asyncio.sleep(0.1, loop=self.loop)

            # Disconnect connection
            yield from self.connection.close()

            # With no_ack=True we should be able to get last message
            msg = yield from consumer.get()
            self.assertEqual(msg.body, b"Test message before disconnect")

            # Any more will raise the error
            with self.assertRaises(ClientConnectionClosed):
                yield from consumer.get()

        self._run_test(go())

    def test_queue_disconnect_client_consumer_ack(self):
        @asyncio.coroutine
        def go():
            consumer = yield from self.queue.queued_consumer(no_ack=False)

            message = asynqp.Message(b"Test message1")
            self.exchange.publish(message, self.queue.name)

            msg = yield from consumer.get()
            self.assertEqual(msg.body, b"Test message1")
            msg.ack()

            message = asynqp.Message(b"Test message before disconnect")
            self.exchange.publish(message, self.queue.name)

            for i in range(20):
                if not consumer.empty():
                    break
                yield from asyncio.sleep(0.1, loop=self.loop)

            # Disconnect connection
            yield from self.connection.close()

            # With no_ack=False we should raise right away without last msg
            with self.assertRaises(ClientConnectionClosed):
                yield from consumer.get()

        self._run_test(go())

    def test_queue_disconnect_server_consumer(self):
        @asyncio.coroutine
        def go():
            consumer = yield from self.queue.queued_consumer(no_ack=True)

            message = asynqp.Message(b"Test message1")
            self.exchange.publish(message, self.queue.name)

            x = yield from consumer.get()
            self.assertEqual(x.body, b"Test message1")

            # Emulate connection lost
            try:
                self.connection.protocol.connection_lost(ConnectionError)
            except ConnectionLostError:
                pass
            self.connection.protocol.transport.close()

            with self.assertRaises(ConnectionLostError):
                yield from consumer.get()

        self._run_test(go())
