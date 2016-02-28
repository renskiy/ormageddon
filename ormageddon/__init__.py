import asyncio
import contextlib
import weakref

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
        with contextlib.suppress(RuntimeError):
            # ignore asyncio current task absence
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

    async def process_row(self, row):
        row = await row
        if not row:
            self._populated = True
            if not getattr(self.cursor, 'name', None):
                self.cursor.close()
            raise StopAsyncIteration
        return super().process_row(row)

    async def iterate(self):
        cursor = self._cursor = self._cursor or await self.cursor
        with patch(self, 'cursor', cursor):
            return await super().iterate()

    def __iter__(self):
        assert self._populated, "Can't iterate over not executed query"
        return super().__iter__()

    async def __aiter__(self):
        return ResultIterator(self)


class Transaction:

    def __init__(self, db):
        self.db = db
        self._autocommit = db.get_autocommit()
        self._connection = None

    def _get_connection(self):
        if self._connection is None:
            raise RuntimeError('Transaction not started yet, use start()')
        return self._connection

    def _set_connection(self, connection):
        self._connection = connection

    connection = property(_get_connection, _set_connection)

    def disable_autocommit(self):
        self.db.set_autocommit(False)

    def restore_autocommit(self):
        self.db.set_autocommit(self._autocommit)


class PostgresqlDatabase(peewee.PostgresqlDatabase):

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = loop or asyncio.get_event_loop()
        self.pool = asyncio.ensure_future(self._create_pool(), loop=self.loop)
        self.__local = TaskConnectionLocal(loop=self.loop, strict=False)
        super().__init__(*args, **kwargs)

    async def _create_pool(self):
        try:
            return await aiopg.create_pool(
                loop=self.loop,
                database=self.database,
                **self.connect_kwargs
            )
        except:
            self.loop.stop()
            raise

    async def get_conn(self):
        pool = await self.pool
        connection = await pool.acquire()
        return connection

    async def get_cursor(
        self,
        connection=None,
        release_new_connection=True,
        force_release_connection=False,
    ):
        need_release_connection = release_new_connection and not connection
        connection = connection or await self.get_conn()
        cursor = await connection.cursor()
        if need_release_connection or force_release_connection:
            pool = await self.pool
            weakref.finalize(cursor, pool.release, connection)
        return cursor

    def get_result_wrapper(self, wrapper_type):
        if wrapper_type == peewee.RESULTS_NAIVE:
            return NaiveQueryResultWrapper

    def set_autocommit(self, autocommit):
        self.__local.autocommit = autocommit

    def get_autocommit(self):
        if self.__local.autocommit is None:
            self.set_autocommit(self.autocommit)
        return self.__local.autocommit

    def transaction(self):
        return self.begin()

    def push_transaction(self, transaction):
        self.__local.transactions.append(transaction)

    def pop_transaction(self):
        self.__local.transactions.pop()

    def transaction_depth(self):
        return len(self.__local.transactions)

    def get_transaction(self) -> Transaction:
        if self.transaction_depth() == 1:
            return self.__local.transactions[-1]

    async def _execute_sql(
        self,
        sql,
        params=None,
        require_commit=True,
        connection=None,
    ):
        with self.exception_wrapper():
            cursor = await self.get_cursor(connection)
            await cursor.execute(sql, params)
            if require_commit:
                await self._commit(cursor.connection)
        return cursor

    def execute_sql(self, sql, params=None, require_commit=True):
        transaction = self.get_transaction()
        connection = transaction and transaction.connection
        return self._execute_sql(
            sql,
            params=params,
            require_commit=require_commit and self.get_autocommit(),
            connection=connection,
        )

    async def _begin(self, transaction):
        cursor = await self.get_cursor(release_new_connection=False)
        await cursor.execute('BEGIN')
        transaction.connection = cursor.connection
        return transaction

    def begin(self):
        transaction = self.get_transaction()
        if not transaction:
            transaction = Transaction(self)
            transaction.disable_autocommit()
            self.push_transaction(transaction)
            return self._begin(transaction)
        # TODO raise warning?
        future = asyncio.Future(loop=self.loop)
        future.set_result(transaction)
        return future

    async def _commit(self, connection, force_release_connection=False):
        cursor = await self.get_cursor(
            connection,
            force_release_connection=force_release_connection,
        )
        await cursor.execute('COMMIT')

    def commit(self, transaction=None):
        transaction = transaction or self.get_transaction()
        if transaction:
            self.pop_transaction()
            transaction.restore_autocommit()
            return self._commit(
                connection=transaction.connection,
                force_release_connection=True,
            )
        # TODO raise warning?
        future = asyncio.Future(loop=self.loop)
        future.set_result(None)
        return future















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
