import asyncio
import contextlib

import aiopg
import peewee
import tasklocals


@contextlib.contextmanager
def patch(obj, attr, value, default=None):
    original = getattr(obj, attr, default)
    setattr(obj, attr, value)
    yield
    setattr(obj, attr, original)


class TaskConnectionLocal(peewee._BaseConnectionLocal, tasklocals.local):

    def __init__(self, **kwargs):
        # ignore asyncio current task absence
        with contextlib.suppress(RuntimeError):
            super().__init__()


class Model(peewee.Model):

    @classmethod
    def select(cls, *selection):
        query = super().select(*selection)
        query.__class__ = SelectQuery
        return query


class SelectQuery(peewee.SelectQuery):

    async def __aiter__(self):
        return await self.execute().__aiter__()

    async def get(self):
        try:
            return await super().get()
        except StopAsyncIteration:
            raise self.model_class.DoesNotExist(
                'Instance matching query does not exist:\nSQL: %s\nPARAMS: %s'
                % self.sql())

    async def first(self):
        with contextlib.suppress(self.model_class.DoesNotExist):
            return await self.get()


class ResultIterator(peewee.ResultIterator):

    async def __anext__(self):
        try:
            result = self.next()
            if not self.qrw._populated:
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


class NaiveQueryResultWrapper(peewee.NaiveQueryResultWrapper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cursor = None

    async def iterate(self):
        cursor = self._cursor = self._cursor or await self.cursor
        row = await cursor.fetchone()
        with contextlib.ExitStack() as context_stack:
            context_stack.enter_context(patch(self, 'cursor', cursor))
            context_stack.enter_context(patch(cursor, 'fetchone', lambda: row))
            try:
                return super().iterate()
            except StopIteration:
                raise StopAsyncIteration

    def __iter__(self):
        assert self._populated
        return super().__iter__()

    async def __aiter__(self):
        return ResultIterator(self)

    # async def __anext__(self):
    #     print('NaiveQueryResultWrapper.__anext__')
    #     try:
    #         result = await self.next()
    #         self._result_cache[-1] = result
    #         return result
    #     except StopAsyncIteration:
    #         self._ct -= 1
    #         self._idx -= 1
    #         self._result_cache.pop()
    #         raise
    #     except StopIteration:
    #         raise StopAsyncIteration


class PostgresqlDatabase(peewee.PostgresqlDatabase):

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = loop or asyncio.get_event_loop()
        self.pool = asyncio.ensure_future(self._create_pool(), loop=self.loop)
        self.__local = TaskConnectionLocal(loop=self.loop, strict=False)
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
            connection = await self.get_conn()
        return await connection.cursor()

    def get_result_wrapper(self, wrapper_type):
        if wrapper_type == peewee.RESULTS_NAIVE:
            return NaiveQueryResultWrapper

    def push_transaction(self, transaction):
        self.__local.transactions.append(transaction)

    def pop_transaction(self):
        return self.__local.transactions.pop()

    def transaction_depth(self):
        return len(self.__local.transactions)

    def get_transaction(self):
        if not self.get_autocommit():
            try:
                return self.__local.transactions[-1]
            except IndexError:
                raise RuntimeError('With autocommit=False `execute_sql()` must be called within the transaction')

    async def _execute_sql(self,
                           sql,
                           params=None,
                           require_commit=True,
                           transaction=None,
                           ):
        with self.exception_wrapper():
            if transaction:
                cursor = await transaction.get_cursor()
            else:
                cursor = await self.get_cursor()
            await cursor.execute(sql, params)
            if require_commit and not transaction:
                await self.commit(cursor.connection)
                (await self.pool).release(cursor.connection)
        return cursor

    def execute_sql(self, sql, params=None, require_commit=True):
        transaction = self.get_transaction()
        return self._execute_sql(
            sql,
            params=params,
            require_commit=require_commit,
            transaction=transaction,
        )

    async def _commit(self, connection):
        cursor = await self.get_cursor(connection)
        with contextlib.closing(cursor):
            await cursor.execute('COMMIT')

    def commit(self, connection=None):
        # TODO: if not connection use current transaction
        return self._commit(connection=connection)














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
