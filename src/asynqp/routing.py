import asyncio
import collections
from . import frames
from .log import log


class Dispatcher(object):
    def __init__(self):
        self.handlers = {}

    def add_handler(self, channel_id, handler):
        self.handlers[channel_id] = handler

    def remove_handler(self, channel_id):
        del self.handlers[channel_id]

    def dispatch(self, frame):
        if isinstance(frame, frames.HeartbeatFrame):
            return
        handler = self.handlers[frame.channel_id]
        handler(frame)

    def dispatch_all(self, frame):
        for handler in self.handlers.values():
            handler(frame)


class Sender(object):
    def __init__(self, channel_id, protocol):
        self.channel_id = channel_id
        self.protocol = protocol

    def send_method(self, method):
        self.protocol.send_method(self.channel_id, method)


class Actor(object):
    def __init__(self, synchroniser, sender, *, loop):
        self._loop = loop
        self.synchroniser = synchroniser
        self.sender = sender

    def handle(self, frame):
        try:
            meth = getattr(self, 'handle_' + type(frame).__name__)
        except AttributeError:
            meth = getattr(self, 'handle_' + type(frame.payload).__name__)

        meth(frame)


class Synchroniser(object):

    def __init__(self, *, loop):
        self._loop = loop
        self._futures = collections.defaultdict(collections.deque)
        self.connection_exc = None

    def await(self, *expected_methods):
        fut = asyncio.Future(loop=self._loop)

        if self.connection_exc is not None:
            fut.set_exception(self.connection_exc)
            return fut

        for method in expected_methods:
            self._futures[method].append((fut, expected_methods))

        return fut

    def notify(self, method, result=None):
        try:
            fut, methods = self._futures[method].popleft()
        except IndexError:
            # XXX: we can't just ignore this.
            log.error("Got an unexpected method notification %s %s",
                      method, result)
            return

        if fut.done():
            # We can have cancelled future if the ``Synchroniser.await()`` call
            # was cancelled. We must still process the matching frame so we
            # preserve frame order.
            assert fut.cancelled(), \
                "We should never have done but not cancelled futures"
        else:
            fut.set_result(result)

        # Awaited for any frame to arrive. For example:
        #   (spec.BasicGetOK, spec.BasicGetEmpty)
        if len(methods) > 1:
            # Remove other futures from waiters.
            for m in methods:
                if m == method:
                    continue
                poped_fut, _ = self._futures[m].popleft()
                assert poped_fut is fut

    def killall(self, exc):
        """ Connection/Channel was closed. All subsequent and ongoing requests
            should raise an error
        """
        self.connection_exc = exc
        # Set an exception for all others
        for method, futs in self._futures.items():
            for fut, _ in futs:
                if fut.done():
                    continue
                fut.set_exception(exc)
        self._futures.clear()
