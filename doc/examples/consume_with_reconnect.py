import asyncio
import asynqp
import logging

log = logging.getLogger(__name__)


class DataIndexer:

    RECONNECT_TIMEOUT = 1

    def __init__(self, *, loop, **params):
        self.params = params
        self.loop = loop

    # Connect/reconnect logic

    @asyncio.coroutine
    def start(self):
        # connect to the RabbitMQ broker
        yield from self.connect()
        log.info('Started indexing')
        try:
            while True:
                try:
                    yield from self.index()
                except asynqp.AMQPConnectionError as err:
                    log.warning('Connection lost. Error %s. Reconnecting to '
                                'rabbitmq...', err)
                    # Wait for reconnect.
                    yield from self.reconnect()
        except asyncio.CancelledError:
            pass
        finally:
            yield from self.disconnect()

    @asyncio.coroutine
    def connect(self):
        self.connection = yield from asynqp.connect(
            loop=self.loop, **self.params)
        # Open a communications channel
        self.channel = yield from self.connection.open_channel()
        # Create a queue and an exchange on the broker
        self.queue = yield from self.channel.declare_queue('some.queue')

    @asyncio.coroutine
    def disconnect(self):
        yield from self.channel.close()
        yield from self.connection.close()

    @asyncio.coroutine
    def reconnect(self):
        while True:
            try:
                yield from self.connect()
            except (ConnectionError, OSError):
                log.warning(
                    'Failed to reconnect to rabbitmq. Try again in '
                    '{} seconds...'.format(self.RECONNECT_TIMEOUT))
                yield from asyncio.sleep(
                    self.RECONNECT_TIMEOUT, loop=self.loop)
            else:
                log.info('Successfully reconnected to rabbitmq')
                break

    # Indexer logic

    @asyncio.coroutine
    def index(self):
        consumer = yield from self.queue.queued_consumer()
        while True:
            msg = yield from consumer.get()

            try:
                yield from self._index(msg)
            except asyncio.CancelledError:
                # We can't be sure, that the message was processed, most likely
                # not.
                msg.reject()
            except Exception:
                log.error(
                    "Something bad happend while processing msg=%s",
                    msg.body, exc_info=True)
                msg.reject()

    @asyncio.coroutine
    def _index(self, msg):
        # Index message.
        # Most likely you will put a try/except here to work on Database
        # specific errors, like AlreadyIndexed, when you will want to call
        # `msk.ack()`, rather than `msg.reject()`
        # For example:
        #
        #     try:
        #         yield from self.db_driver.index(
        #             body=msg.body
        #         )
        #     except AlreadyIndexed:
        #         # Most likely we had a not clean shutdown and event was
        #         # redelivered.
        #         msg.ack()
        #     except ValidationError:
        #         # Most likely the msg is broken. Requeueing it will just
        #         # mess the queue, so let's dropletter it.
        #         log.error('Failed validation %s', msg.body, exc_info=True)
        #         msg.reject(requeue=False)
        #     except ConnectionError:
        #         # The DB is down, maybe on reboot. Lets trottle number of
        #         # requests so it comes back faster
        #         log.warning('DB is down')
        #         msg.reject()
        #         yield from asyncio.sleep(self.DB_WAITER, loop=self.loop)
        #     else:
        #         msg.ack()
        print(msg, msg.body)
        msg.ack()


def main():
    # My preference to disable global event_loop.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    logging.basicConfig(level=logging.INFO)

    indexer = DataIndexer(
        host='localhost',
        port=5672,
        username='guest',
        password='guest',
        loop=loop
    )
    # Start main indexing task in the background
    main_task = loop.create_task(indexer.start())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        main_task.cancel()
        # Note: Always try to run the loop to the end of task after it's
        #       cancelation. This can be very critical if you use
        #       `yield from` in finally blocks. For example:
        #
        #           yield from some_lock.acquire()
        #           try:
        #               yield from index_message(msg)
        #           finally:
        #               yield from some_lock.release()
        #
        #       This block of code may not release the lock if we don't run
        #       our loop long enough for it to finish all finally blocks.
        loop.run_until_complete(main_task)
    loop.close()

if __name__ == "__main__":
    main()
