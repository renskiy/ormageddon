import asyncio
import weakref

import aiopg
import peewee

from cached_property import cached_property

from ormageddon.db import TaskConnectionLocal
from ormageddon.transaction import Transaction, TransactionContext
from ormageddon.wrappers import NaiveQueryResultWrapper


class PostgresqlDatabase(peewee.PostgresqlDatabase):

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = loop or asyncio.get_event_loop()
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

    @cached_property
    def pool(self):
        return asyncio.ensure_future(self._create_pool(), loop=self.loop)

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
