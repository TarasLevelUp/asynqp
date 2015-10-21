import asyncio
import sys
from . import spec
from . import routing
from .channel import ChannelFactory
from .exceptions import (
    AlreadyClosed, ClientConnectionClosed, ServerConnectionClosed)
from .log import log


class Connection(object):
    """
    Manage connections to AMQP brokers.

    A :class:`Connection` is a long-lasting mode of communication with a remote server.
    Each connection occupies a single TCP connection, and may carry multiple :class:`Channels <Channel>`.
    A connection communicates with a single virtual host on the server; virtual hosts are
    sandboxed and may not communicate with one another.

    Applications are advised to use one connection for each AMQP peer it needs to communicate with;
    if you need to perform multiple concurrent tasks you should open multiple channels.

    Connections are created using :func:`asynqp.connect() <connect>`.

    .. attribute:: closed

        a :class:`~asyncio.Future` which is done when the handshake to close the connection has finished

    .. attribute:: transport

        The :class:`~asyncio.BaseTransport` over which the connection is communicating with the server

    .. attribute:: protocol

        The :class:`~asyncio.Protocol` which is paired with the transport
    """
    def __init__(self, loop, transport, protocol, synchroniser, sender, dispatcher, connection_info):
        self._loop = loop
        self.synchroniser = synchroniser
        self.sender = sender
        self.channel_factory = ChannelFactory(loop, protocol, dispatcher, connection_info)
        self.connection_info = connection_info

        self.transport = transport
        self.protocol = protocol
        # Indicates, that close was initiated by client
        self._closing = False
        # List of created channels
        self._close_waiters = []

    @asyncio.coroutine
    def open_channel(self):
        """
        Open a new channel on this connection.

        This method is a :ref:`coroutine <coroutine>`.

        :return: The new :class:`Channel` object.
        """
        channel = yield from self.channel_factory.open()
        return channel

    @asyncio.coroutine
    def close(self):
        """
        Close the connection by handshaking with the server.

        This method is a :ref:`coroutine <coroutine>`.
        """
        if not self._closing:
            self._closing = True
            # Let the ConnectionActor do the actual close operations.
            # It will do the work on CloseOK
            try:
                self.sender.send_Close(
                    0, 'Connection closed by application', 0, 0)
                yield from self.synchroniser.await(spec.ConnectionCloseOK)
            except AlreadyClosed:
                # For example if both sides want to close or the connection
                # is closed.
                pass
        else:
            log.warn("Called `close` on already closed connection...")
        # Wait for all components to close
        if self._close_waiters:
            yield from asyncio.gather(*self._close_waiters, loop=self._loop)


@asyncio.coroutine
def open_connection(loop, transport, protocol, dispatcher, connection_info):
    synchroniser = routing.Synchroniser(loop=loop)

    sender = ConnectionMethodSender(protocol)
    connection = Connection(loop, transport, protocol, synchroniser, sender, dispatcher, connection_info)
    actor = ConnectionActor(synchroniser, sender, protocol, connection, loop=loop)
    reader = routing.QueuedReader(actor, loop=loop)

    try:
        dispatcher.add_handler(0, reader.feed)
        protocol.send_protocol_header()
        reader.ready()

        yield from synchroniser.await(spec.ConnectionStart)
        sender.send_StartOK(
            {"product": "asynqp",
             "version": "0.1",  # todo: use pkg_resources to inspect the package
             "platform": sys.version},
            'AMQPLAIN',
            {'LOGIN': connection_info['username'], 'PASSWORD': connection_info['password']},
            'en_US'
        )
        reader.ready()

        frame = yield from synchroniser.await(spec.ConnectionTune)
        # just agree with whatever the server wants. Make this configurable in future
        connection_info['frame_max'] = frame.payload.frame_max
        heartbeat_interval = frame.payload.heartbeat
        sender.send_TuneOK(frame.payload.channel_max, frame.payload.frame_max, heartbeat_interval)

        sender.send_Open(connection_info['virtual_host'])
        protocol.start_heartbeat(heartbeat_interval)
        reader.ready()

        yield from synchroniser.await(spec.ConnectionOpenOK)
        reader.ready()
    except:
        dispatcher.remove_handler(0)
        raise
    return connection


class ConnectionActor(routing.Actor):
    def __init__(self, synchroniser, sender, protocol, connection, *, loop=None):
        super().__init__(synchroniser, sender, loop=loop)
        self.protocol = protocol
        self.connection = connection

    def handle_ConnectionStart(self, frame):
        self.synchroniser.notify(spec.ConnectionStart)

    def handle_ConnectionTune(self, frame):
        self.synchroniser.notify(spec.ConnectionTune, frame)

    def handle_ConnectionOpenOK(self, frame):
        self.synchroniser.notify(spec.ConnectionOpenOK)

    def handle_PoisonPillFrame(self, frame):
        """ Is sent in case protocol lost connection to server."""
        # Make sure all `close` calls don't deadlock
        self.connection._closing = True
        exc = frame.exception
        # Transport is already closed
        self._close_all(exc)

    def handle_ConnectionClose(self, frame):
        """ AMQP server closed the channel with an error """
        # Notify server we are OK to close.
        self.sender.send_CloseOK()
        self.connection._closing = True
        exc = ServerConnectionClosed()
        self._close_all(exc)

        # Don't close transport right away, as CloseOK might not get to server
        # yet. At least give the loop a spin before we do so.
        # TODO: After FlowControl is implemented change this to drain and close
        self._loop.call_soon(self.protocol.transport.close)

    def handle_ConnectionCloseOK(self, frame):
        self.synchroniser.notify(spec.ConnectionCloseOK)
        exc = ClientConnectionClosed()
        self._close_all(exc)
        # We already agread with server on closing, so lets do it right away
        self.protocol.transport.close()

    def _close_all(self, exc):
        # Close heartbeat
        self.protocol.heartbeat_monitor.stop()
        self.connection._close_waiters.append(
            self.protocol.heartbeat_monitor.wait_closed())
        # If there were anyone who expected an `*-OK` kill them, as no data
        # will follow after close
        self.synchroniser.killall(exc)
        self.sender.killall(exc)


class ConnectionMethodSender(routing.Sender):
    def __init__(self, protocol):
        super().__init__(0, protocol)

    def send_StartOK(self, client_properties, mechanism, response, locale):
        method = spec.ConnectionStartOK(client_properties, mechanism, response, locale)
        self.send_method(method)

    def send_TuneOK(self, channel_max, frame_max, heartbeat):
        self.send_method(spec.ConnectionTuneOK(channel_max, frame_max, heartbeat))

    def send_Open(self, virtual_host):
        self.send_method(spec.ConnectionOpen(virtual_host, '', False))

    def send_Close(self, status_code, message, class_id, method_id):
        method = spec.ConnectionClose(status_code, message, class_id, method_id)
        self.send_method(method)

    def send_CloseOK(self):
        self.send_method(spec.ConnectionCloseOK())
