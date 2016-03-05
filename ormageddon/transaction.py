__all__ = [
    'TransactionContext',
    'Transaction',
]


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
