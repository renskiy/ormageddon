import asyncio
import contextlib

import peewee
import tasklocals

__all__ = [
    'PostgresqlDatabase',
]


class TaskConnectionLocal(peewee._BaseConnectionLocal, tasklocals.local):

    def __init__(self, **kwargs):
        # ignore asyncio current task absence
        with contextlib.suppress(RuntimeError):
            super().__init__()


class Database(peewee.Database):

    def __init__(self, *args, loop=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = loop or asyncio.get_event_loop()
        self.__local = TaskConnectionLocal(loop=self.loop)

    def set_autocommit(self, autocommit):
        self.__local.autocommit = autocommit

    def get_autocommit(self):
        if self.__local.autocommit is None:
            self.set_autocommit(self.autocommit)
        return self.__local.autocommit

    def push_transaction(self, transaction):
        self.__local.transactions.append(transaction)

    def pop_transaction(self):
        self.__local.transactions.pop()

    def transaction_depth(self):
        return len(self.__local.transactions)

    def get_transaction(self):
        if self.transaction_depth() == 1:
            return self.__local.transactions[-1]


class MisconfiguredDatabase:

    def __init__(self, error):
        self.error = error

    def __call__(self, *args, **kwargs):
        raise self.error


try:
    from .postgresql import PostgresqlDatabase
except ImportError as error:
    PostgresqlDatabase = MisconfiguredDatabase(error)
