import contextlib
import inspect

import peewee

from ormageddon.query import *
from ormageddon.utils import patch

__all__ = [
    'Model',
]


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

    @classmethod
    def update(cls, *args, **kwargs):
        query = super().update(*args, **kwargs)
        query.__class__ = UpdateQuery
        return query

    @classmethod
    def delete(cls):
        return DeleteQuery(cls)

    async def delete_instance(self, recursive=False, delete_nullable=False):
        def executor(execute):
            def _execute(query):
                queries_to_execute.append(execute(query))
            return _execute
        queries_to_execute = []
        with contextlib.ExitStack() as exit_stack:
            exit_stack.enter_context(patch(DeleteQuery, 'execute', executor(DeleteQuery.execute)))
            exit_stack.enter_context(patch(UpdateQuery, 'execute', executor(UpdateQuery.execute)))
            super().delete_instance(recursive=recursive, delete_nullable=delete_nullable)
        for future in queries_to_execute:
            result = await future
        return result

    async def save(self, force_insert=False, only=None):
        result = super().save(force_insert=force_insert, only=only)
        if self._meta.primary_key is not False:
            pk_value = self._get_pk_value()
            if inspect.isawaitable(pk_value):
                self._set_pk_value(await pk_value)
        if inspect.isawaitable(result):
            return await result
        return result
