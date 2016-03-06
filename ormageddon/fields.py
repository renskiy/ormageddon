import inspect

import peewee

__all__ = [
    'PrimaryKeyField',
    'IntegerField',
]


class Field(peewee.Field):

    async def async_python_value(self, value):
        return super().python_value(await value)

    def python_value(self, value):
        if inspect.isawaitable(value):
            return self.async_python_value(value)
        return super().python_value(value)


class PrimaryKeyField(peewee.PrimaryKeyField, Field):
    pass


class IntegerField(peewee.IntegerField, Field):
    pass
