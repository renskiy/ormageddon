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


class LazyCursor:

    def __init__(self, cursor_getter, loop=None):
        self._real_cursor = asyncio.Future(loop=loop)
        self.cursor_getter = cursor_getter

    @property
    async def real_cursor(self):
        if not self._real_cursor.done():
            self._real_cursor.set_result(await self.cursor_getter())
        return self._real_cursor.result()

    async def fetchone(self):
        return await (await self.real_cursor).fetchone()

    async def fetchall(self):
        return await (await self.real_cursor).fetchall()


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
        cursor_getter = self._execute
        with contextlib.ExitStack() as exit_stack:
            exit_stack.enter_context(patch(self, '_execute', lambda: LazyCursor(cursor_getter, loop=loop)))
            exit_stack.enter_context(patch(peewee, 'map', functools.partial(map_async, loop=loop), map))
            exit_stack.enter_context(patch(peewee, 'zip', functools.partial(zip_async, loop=loop), zip))
            result = super().execute()
            if inspect.isawaitable(result):
                result = await result
            elif isinstance(result, list):
                result = await asyncio.gather(*result, loop=loop)
            return result
