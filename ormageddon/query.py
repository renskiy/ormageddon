import asyncio
import contextlib
import functools
import inspect

import peewee

from ormageddon.utils import *

__all__ = [
    'SelectQuery',
    'InsertQuery',
]


def _map(callback, *iterables, loop=None):
    if any(map(inspect.isawaitable, iterables)):
        return map_async(callback, *iterables, loop=loop)
    return map(callback, *iterables)


def _zip(*iterables, loop=None):
    assert not all(map(inspect.isawaitable, iterables))
    return zip(*ensure_iterables(*iterables, loop=loop))


class LazyCursor:

    __slots__ = ('cursor', )

    def __init__(self, fetch_cursor, loop=None):
        self.cursor = asyncio.ensure_future(
            fetch_cursor(),
            loop=loop,
        )

    async def fetchone(self):
        return await (await self.cursor).fetchone()

    async def fetchall(self):
        return await (await self.cursor).fetchall()


class Query(peewee.Query):

    def scalar(self, as_tuple=False, convert=False):
        pass  # TODO


class SelectQuery(Query, peewee.SelectQuery):

    async def __aiter__(self):
        return await self.execute().__aiter__()

    def _first_result(self):
        return next(self.execute())

    async def get(self):
        clone = self.clone()
        clone._limit = 1
        with contextlib.suppress(StopAsyncIteration):
            return await clone._first_result()
        raise self.model_class.DoesNotExist(
            'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
            % self.sql())

    async def first(self):
        with contextlib.suppress(StopAsyncIteration):
            return await self._first_result()

    async def _getitem(self):
        with contextlib.suppress(StopAsyncIteration):
            return await self._first_result()
        raise IndexError

    def __getitem__(self, item):
        """
        Behavior of this method is slightly different from the original one
        because of we are considering `slice.start` while `peewee` is not
        """
        clone = self.clone()
        if isinstance(item, slice):
            assert item.step is None, "Slicing with step is not supported"
            if item.stop is not None and item.stop <= 0:
                raise ValueError("stop must be positive if any")
            if item.start is not None and item.start < 0:
                raise ValueError("start can't be negative")
            if item.start is not None and item.stop is not None and item.start >= item.stop:
                raise ValueError("stop must be greater then start if any")
            clone._offset = item.start
            clone._limit = item.stop and (item.stop - (item.start or 0))
            return clone
        else:
            clone._offset = item
            clone._limit = 1
            return clone._getitem()

    def __len__(self):
        raise NotImplementedError("Can't get len of the result in async mode")

    def __await__(self):
        return (yield from asyncio.ensure_future(
            self.first(),
            loop=self.database.loop,
        ))


class InsertQuery(Query, peewee.InsertQuery):

    def _insert_with_loop(self):
        pass  # TODO

    async def execute(self):
        loop = self.database.loop
        fetch_cursor = self._execute
        with contextlib.ExitStack() as exit_stack:
            exit_stack.enter_context(patch(self, '_execute', lambda: LazyCursor(fetch_cursor, loop=loop)))
            exit_stack.enter_context(patch(peewee, 'map', functools.partial(_map, loop=loop), map))
            exit_stack.enter_context(patch(peewee, 'zip', functools.partial(_zip, loop=loop), zip))
            result = super().execute()
            if inspect.isawaitable(result):
                result = await result
            elif isinstance(result, list):
                result = await asyncio.gather(*result, loop=loop)
            return result
