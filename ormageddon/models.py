import peewee

from ormageddon.query import *

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

    async def save(self, force_insert=False, only=None):
        result = super().save(force_insert=force_insert, only=only)
        pk_field = self._meta.primary_key
        if pk_field is not None:
            pk_value = await getattr(self, pk_field.name)
            setattr(self, pk_field.name, pk_value)
        return result
