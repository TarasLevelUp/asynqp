from ._exceptions import AMQPError
from .spec import EXCEPTIONS, CONSTANTS_INVERSE


__all__ = [
    "AMQPError",
    "ConnectionLostError",
    "UndeliverableMessage",
    "Deleted"
]
__all__.extend(EXCEPTIONS.keys())


class AlreadyClosed(Exception):
    """ Raised when issuing commands on closed Channel/Connection """


class ConnectionLostError(AlreadyClosed, ConnectionError):
    """ Connection was closed unexpectedly """

    def __init__(self, message, exc=None):
        super().__init__(message)
        self.original_exc = exc


class ClientConnectionClosed(AlreadyClosed):
    """ Connection was closed by client """


class ClientChannelClosed(AlreadyClosed):
    """ Channel was closed by client """


class ServerConnectionClosed(AlreadyClosed):
    """ Connection was closed by server """


class UndeliverableMessage(ValueError):
    pass


class Deleted(ValueError):
    pass


globals().update(EXCEPTIONS)


def _get_exception_type(reply_code):
    name = CONSTANTS_INVERSE[reply_code]
    classname = ''.join([x.capitalize() for x in name.split('_')])
    return EXCEPTIONS[classname]
