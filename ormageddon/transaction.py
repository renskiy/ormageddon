__all__ = [
    'Transaction',
    'TransactionContext',
]


class TransactionContext:

    def __init__(self, transaction):
        self.transaction = transaction
        self._transaction = None

    async def __aenter__(self):
        self._transaction = transaction = await self.transaction
        return transaction

    def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            return self._transaction.commit(close_transaction=True)
        else:
            return self._transaction.rollback(close_transaction=True)


class Transaction:

    def __init__(self, db):
        self.db = db
        self._autocommit = db.get_autocommit()
        self._connection = None

    @property
    def started(self):
        return self._connection is not None

    def _get_connection(self):
        assert self.started
        return self._connection

    def _set_connection(self, connection):
        self._connection = connection

    connection = property(_get_connection, _set_connection)

    def disable_autocommit(self):
        self.db.set_autocommit(False)

    def restore_autocommit(self):
        self.db.set_autocommit(self._autocommit)

    def commit(self, close_transaction=False):
        return self.db.commit(close_transaction=close_transaction)

    def rollback(self, close_transaction=False):
        return self.db.rollback(close_transaction=close_transaction)
