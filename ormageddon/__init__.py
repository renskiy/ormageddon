import asyncio
import contextlib
import weakref

import aiopg
import peewee
import tasklocals

from ormageddon.fields import *
from ormageddon.query import *
from ormageddon.utils import *


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
