import inspect

import peewee

from ormageddon.utils import patch

__all__ = [
    'NaiveQueryResultWrapper',
]


class ResultIterator(peewee.ResultIterator):

    async def __anext__(self):
        try:
            result = self.__next__()
            if inspect.isawaitable(result):
                result = await result
                self.qrw._result_cache[-1] = result
            return result
        except StopAsyncIteration:
            self.qrw._result_cache.pop()
            self.qrw._ct -= 1
            self._idx -= 1
            raise
        except StopIteration:
            raise StopAsyncIteration


class QueryResultWrapper(peewee.QueryResultWrapper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cursor = None

    async def process_row(self, row):
        row = await row
        if not row:
            self._populated = True
            if not getattr(self._cursor, 'name', None):
                self._cursor.close()
            raise StopAsyncIteration
        return super().process_row(row)

    async def iterate(self):
        cursor = self._cursor = self._cursor or await self.cursor
        with patch(self, 'cursor', cursor):
            return await super().iterate()

    def __iter__(self):
        raise NotImplementedError

    async def __aiter__(self):
        return ResultIterator(self)

    def __len__(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    def next(self):
        return self.__next__()

    def iterator(self):
        raise NotImplementedError

    @property
    async def count(self):
        await self.fill_cache()
        return self._ct

    async def fill_cache(self, n=None):
        index = 0
        delta = 0 if n is None else 1
        async for _ in self:
            index += delta
            if index == n:
                break


class NaiveQueryResultWrapper(QueryResultWrapper, peewee.NaiveQueryResultWrapper):
    pass
