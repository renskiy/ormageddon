import asyncio
import weakref

import aiopg
import peewee

from cached_property import cached_property

from ormageddon.db import Database
from ormageddon.transaction import Transaction, TransactionContext
from ormageddon.wrappers import NaiveQueryResultWrapper


class PostgresqlDatabase(peewee.PostgresqlDatabase, Database):

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

    def transaction(self):
        return TransactionContext(self.begin())

    async def _begin(self, transaction):
        connection = transaction.started and transaction.connection or None
        cursor = await self.get_cursor(
            connection=connection,
            release_new_connection=False,
        )
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

    async def _restart_transaction(self, transaction, commit_or_rollback):
        await commit_or_rollback(connection=transaction.connection)
        await self._begin(transaction)

    def commit_or_rollback(
        self,
        commit=False,
        rollback=False,
        close_transaction=True,
    ):
        assert commit ^ rollback, "You must choose either commit or rollback"
        transaction = self.get_transaction()
        if transaction:
            commit_or_rollback = commit and self._commit or rollback and self._rollback
            if close_transaction:
                self.pop_transaction()
                transaction.restore_autocommit()
                return commit_or_rollback(
                    connection=transaction.connection,
                    force_release_connection=True,
                )
            return self._restart_transaction(transaction, commit_or_rollback)
        # TODO raise warning?
        future = asyncio.Future(loop=self.loop)
        future.set_result(None)
        return future

    def commit(self, close_transaction=True):
        return self.commit_or_rollback(
            commit=True,
            close_transaction=close_transaction,
        )

    def rollback(self, close_transaction=True):
        return self.commit_or_rollback(
            rollback=True,
            close_transaction=close_transaction,
        )

    def last_insert_id(self, cursor, model):
        pass  # TODO
