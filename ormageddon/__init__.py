import asyncio
import contextlib

import aiopg
import tasklocals

import peewee


@contextlib.contextmanager
def patch(obj, attr, value, default=None):
    original = getattr(obj, attr, default)
    setattr(obj, attr, value)
    yield
    setattr(obj, attr, original)


async def async_next(async_iterator):
    return await async_iterator.__anext__()


class ResultIterator(peewee.ResultIterator):

    pass


class Model(peewee.Model):

    @classmethod
    def select(cls, *selection):
        query = super().select(*selection)
        query.__class__ = SelectQuery
        return query


class SelectQuery(peewee.SelectQuery):

    async def __aiter__(self):
        return self.execute()

    async def get(self):
        with patch(peewee, 'next', async_next, default=next):
            future = super().get()
        try:
            return await future
        except StopAsyncIteration:
            raise self.model_class.DoesNotExist(
                'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
                % self.sql())


class NaiveQueryResultWrapper(peewee.NaiveQueryResultWrapper):

    async def iterate(self):
        cursor = await self.cursor
        row = await cursor.fetchone()
        with contextlib.ExitStack() as context_stack:
            context_stack.enter_context(patch(self, 'cursor', cursor))
            context_stack.enter_context(patch(cursor, 'fetchone', lambda: row))
            try:
                return super().iterate()
            except StopIteration:
                raise StopAsyncIteration

    # async def __aiter__(self):
    #     return ResultIterator(super().__iter__())

    async def __anext__(self):
        populated = self._populated
        try:
            return await self.next()
        except StopIteration:
            if not populated:
                self._ct -= 1
                self._idx -= 1
                self._result_cache.pop()
            raise StopAsyncIteration


class PostgresqlDatabase(peewee.PostgresqlDatabase):

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = loop or asyncio.get_event_loop()
        self.pool = asyncio.ensure_future(self._create_pool(), loop=self.loop)
        # self.__local = TaskConnectionLocal(loop=self.loop)
        super().__init__(*args, **kwargs)

    async def _create_pool(self):
        return await aiopg.create_pool(
            loop=self.loop,
            database=self.database,
            **self.connect_kwargs
        )

    async def get_conn(self):
        pool = await self.pool
        return await pool.acquire()

    async def get_cursor(self, connection=None):
        if not connection:
            connection = self.get_conn()
        return await (await connection).cursor()

    def get_result_wrapper(self, wrapper_type):
        if wrapper_type == peewee.RESULTS_NAIVE:
            return NaiveQueryResultWrapper

    async def _execute_sql(self, sql, params=None, require_commit=True,
                           transaction=None):
        with self.exception_wrapper():
            cursor = await self.get_cursor()
            await cursor.execute(sql, params)
        return cursor

    def execute_sql(self, sql, params=None, require_commit=True):
        return self._execute_sql(
            sql,
            params=params,
            require_commit=require_commit,
        )















#     def transaction(self):
#         return Transaction(self)
#
#     def push_transaction(self, transaction):
#         self.__local.transactions.append(transaction)
#
#     def pop_transaction(self):
#         return self.__local.transactions.pop()
#
#     def transaction_depth(self):
#         return len(self.__local.transactions)
#
#     @asyncio.coroutine
#     def _async_execute_sql(self, sql, params=None, transaction=None):
#         cursor = yield from (yield from transaction.connection).cursor()
#         print(id(transaction), sql)
#         yield from cursor.execute(sql, parameters=params)
#         return cursor
#
#     def async_execute_sql(self, sql, params=None, require_commit=True):
#         # TODO require_commit?
#         print(asyncio.Task.current_task())
#         transaction = self.pop_transaction()
#         future = asyncio.ensure_future(
#             self._async_execute_sql(
#                 sql,
#                 params=params,
#                 transaction=transaction,
#             ),
#             loop=self.loop,
#         )
#         self.push_transaction(transaction)
#         return future
#
#     @asyncio.coroutine
#     def _begin(self):
#         cursor = yield from self.async_get_cursor()
#         with contextlib.closing(cursor):
#             yield from cursor.execute('BEGIN')
#             return cursor.connection
#
#     def begin(self):
#         return asyncio.ensure_future(self._begin(), loop=self.loop)
#
#     @asyncio.coroutine
#     def _commit(self, connection):
#         cursor = yield from self.async_get_cursor(connection)
#         with contextlib.closing(cursor):
#             yield from cursor.execute('COMMIT')
#
#     def commit(self, connection=None):
#         return asyncio.ensure_future(self._commit(connection), loop=self.loop)
#
#     @asyncio.coroutine
#     def _rollback(self, connection):
#         cursor = yield from self.async_get_cursor(connection)
#         with contextlib.closing(cursor):
#             yield from cursor.execute('ROLLBACK')
#
#     def rollback(self, connection=None):
#         return asyncio.ensure_future(self._rollback(connection), loop=self.loop)
#
#
# class TaskConnectionLocal(peewee._BaseConnectionLocal, tasklocals.local):
#
#     pass
#
#
# class Transaction(peewee.transaction):
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.connection = None
#         self.commit_or_rollback = None
#         self.queries = []
#
#     def _begin(self):
#         self.connection = self.db.begin()
#
#     @property
#     def loop(self):
#         return self.db.loop
#
#     @asyncio.coroutine
#     def get_cursor(self):
#         return (yield from self.db.get_cursor(self.connection))
#
#     @asyncio.coroutine
#     def _commit(self):
#         if self.queries:
#             yield from asyncio.wait(self.queries, loop=self.loop)
#             self.queries.clear()
#         yield from self.db.commit(self.connection)
#
#     def commit(self, begin=True):
#         self.commit_or_rollback = asyncio.ensure_future(
#             self._commit(),
#             loop=self.loop,
#         )
#
#     @asyncio.coroutine
#     def _rollback(self):
#         yield from self.db.rollback(self.connection)
#
#     def rollback(self, begin=True):
#         self.commit_or_rollback = asyncio.ensure_future(
#             self._rollback(),
#             loop=self.loop,
#         )
#
#     @asyncio.coroutine
#     def _release_connection(self):
#         pool = yield from self.db.pool
#         yield from self.commit_or_rollback
#         pool.release(self.connection.result())
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         super().__exit__(exc_type, exc_val, exc_tb)
#         asyncio.ensure_future(self._release_connection())
