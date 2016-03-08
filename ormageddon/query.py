import asyncio
import contextlib
import functools
import inspect

import peewee

from ormageddon.utils import patch, map_async, ensure_iterables

__all__ = [
    'SelectQuery',
    'UpdateQuery',
    'InsertQuery',
]


def _map(callback, *iterables, loop=None):
    if any(map(inspect.isawaitable, iterables)):
        return map_async(callback, *iterables, loop=loop)
    return map(callback, *iterables)


def _zip(*iterables, loop=None):
    assert not all(map(inspect.isawaitable, iterables))
    return zip(*ensure_iterables(*iterables, loop=loop))


class _QueryExecutor:

    __slots__ = ('cursor', 'execute', 'loop')

    def __init__(self, execute, loop=None):
        self.cursor = None
        self.execute = execute
        self.loop = loop

    def __call__(self):
        self.cursor = asyncio.ensure_future(
            self.execute(),
            loop=self.loop,
        )
        return self

    async def fetchone(self):
        return await (await self.cursor).fetchone()

    async def fetchall(self):
        return await (await self.cursor).fetchall()

    @property
    async def rowcount(self):
        return (await self.cursor).rowcount


class Query(peewee.Query):

    def scalar(self, as_tuple=False, convert=False):
        pass  # TODO


class SelectQuery(Query, peewee.SelectQuery):

    def __aiter__(self):
        return self.execute().__aiter__()

    async def _first_result(self):
        async_iterator = await self.__aiter__()
        return await async_iterator.__anext__()

    def get(self):
        clone = self.clone()
        clone._limit = 1
        with contextlib.suppress(StopAsyncIteration):
            return clone._first_result()
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


class UpdateQuery(Query, peewee.UpdateQuery):

    def __aiter__(self):
        if not self.model_class._meta.database.returning_clause:
            raise ValueError('UPDATE queries cannot be iterated over unless '
                             'they specify a RETURNING clause, which is not '
                             'supported by your database.')
        return self.execute().__aiter__()

    def __iter__(self):
        raise NotImplementedError

    def execute(self):
        with patch(self, '_execute', _QueryExecutor(self._execute, loop=self.database.loop)):
            return super().execute()


class InsertQuery(Query, peewee.InsertQuery):

    async def _insert_with_loop(self):
        with patch(self, '_return_id_list', True):
            id_list = await asyncio.gather(
                *super()._insert_with_loop(),
                loop=self.database.loop,
            )
        if self._return_id_list:
            return id_list
        return id_list[-1]

    async def execute(self):
        loop = self.database.loop
        with contextlib.ExitStack() as exit_stack:
            exit_stack.enter_context(patch(self, '_execute', _QueryExecutor(self._execute, loop=loop)))
            exit_stack.enter_context(patch(peewee, 'map', functools.partial(_map, loop=loop), map))
            exit_stack.enter_context(patch(peewee, 'zip', functools.partial(_zip, loop=loop), zip))
            result = super().execute()
            if inspect.isawaitable(result):
                return await result
            elif isinstance(result, list):
                return await asyncio.gather(*result, loop=loop)
            return result
