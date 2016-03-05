import asyncio
import contextlib
import functools
import inspect
import weakref

import aiopg
import peewee
import tasklocals

from ormageddon.fields import *
from ormageddon.utils import *


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

    @classmethod
    def insert(cls, *args, **kwargs):
        query = super().insert(*args, **kwargs)
        query.__class__ = InsertQuery
        return query

    async def save(self, force_insert=False, only=None):
        result = super().save(force_insert=force_insert, only=only)
        pk_field = self._meta.primary_key
        if pk_field is not None:
            pk_value = await getattr(self, pk_field.name)
            setattr(self, pk_field.name, pk_value)
        return result


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
        raise NotImplementedError("Can't iterate over query in async mode")

    async def __aiter__(self):
        return ResultIterator(self)

    @property
    def count(self):
        raise NotImplementedError("Can't count result of query in async mode")


class TransactionContext:

    def __init__(self, transaction):
        self.transaction = transaction
        self._transaction = None

    async def __aenter__(self):
        self._transaction = transaction = await self.transaction
        return transaction

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self._transaction.commit()
        else:
            await self._transaction.rollback()


class Transaction:

    def __init__(self, db):
        self.db = db
        self._autocommit = db.get_autocommit()
        self._connection = None

    def _get_connection(self):
        assert self._connection is not None
        return self._connection

    def _set_connection(self, connection):
        self._connection = connection

    connection = property(_get_connection, _set_connection)

    def disable_autocommit(self):
        self.db.set_autocommit(False)

    def restore_autocommit(self):
        self.db.set_autocommit(self._autocommit)

    async def commit(self):
        await self.db.commit(self)

    async def rollback(self):
        await self.db.rollback(self)


class PostgresqlDatabase(peewee.PostgresqlDatabase):

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = loop or asyncio.get_event_loop()
        self._pool = asyncio.Future(loop=self.loop)
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

    @property
    async def pool(self):
        if not self._pool.done():
            self._pool.set_result(await self._create_pool())
        return self._pool.result()

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
        return TransactionContext(self.begin())

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

    async def _rollback(self, connection, force_release_connection=False):
        cursor = await self.get_cursor(
            connection,
            force_release_connection=force_release_connection,
        )
        await cursor.execute('ROLLBACK')

    def commit_or_rollback(
        self,
        transaction=None,
        commit=False,
        rollback=False,
    ):
        assert commit ^ rollback, "You must choose either commit or rollback"
        transaction = transaction or self.get_transaction()
        if transaction:
            self.pop_transaction()
            transaction.restore_autocommit()
            command = commit and self._commit or rollback and self._rollback
            return command(
                connection=transaction.connection,
                force_release_connection=True,
            )
        # TODO raise warning?
        future = asyncio.Future(loop=self.loop)
        future.set_result(None)
        return future

    def commit(self, transaction=None):
        return self.commit_or_rollback(transaction=transaction, commit=True)

    def rollback(self, transaction=None):
        return self.commit_or_rollback(transaction=transaction, rollback=True)

    def last_insert_id(self, cursor, model):
        pass  # TODO
