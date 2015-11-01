# Module for Python3.5+ to Python3.4 compatibility
import asyncio
import sys


PY_35 = sys.version_info >= (3, 5)


if PY_35:
    from collections.abc import Coroutine
    base = Coroutine
else:
    base = object


# Taken from aiohttp with minor changes.
class _UserCoroutine(base):
    """ For Python3.4 this is just a transparent coroutine proxy
        For Python3.5+ it's a coroutine proxy, but we can also make it a 
        context manager by subclassing. Ie those are equal:

            >>> context = await coro()

            >>> coro_or_context = await _UserCoroutine(coro())
    """

    __slots__ = ('_coro', '_resp')

    def __init__(self, coro):
        self._coro = coro
        self._resp = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    @asyncio.coroutine
    def __iter__(self):
        resp = yield from self._coro
        return resp

    if PY_35:
        def __await__(self):
            resp = yield from self._coro
            return resp


if not PY_35:
    try:
        from asyncio import coroutines
        coroutines._COROUTINE_TYPES += (_UserCoroutine,)
    except:
        pass