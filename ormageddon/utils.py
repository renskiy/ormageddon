import asyncio
import contextlib
import functools
import inspect

__all__ = [
    'patch',
    'map_async',
    'zip_async',
    'future_generator',
]


@contextlib.contextmanager
def patch(obj, attr, value, default=None):
    original = getattr(obj, attr, default)
    setattr(obj, attr, value)
    yield
    setattr(obj, attr, original)


async def _item(future, index):
    return (await future)[index]


def future_generator(future):
    index = 0
    while True:
        yield _item(future, index)
        index += 1


def ensure_iterables(*iterables, loop=None):
    result = []
    all_iterables_are_awaitable = True
    for iterable in iterables:
        if inspect.isawaitable(iterable):
            future = asyncio.ensure_future(iterable, loop=loop)
            result.append(future_generator(future))
        else:
            all_iterables_are_awaitable = False
            result.append(iterable)
    if all_iterables_are_awaitable:
        raise ValueError('There must be at least one common iterator')
    return result


def zip_async(*iterables, loop=None):
    return zip(*ensure_iterables(*iterables, loop=loop))


def make_future(entity, loop=None):
    if inspect.isawaitable(entity):
        return entity
    future = asyncio.Future(loop=loop)
    future.set_result(entity)
    return future


async def _map_async(callback, *iterables, loop=None):
    futures = map(functools.partial(make_future, loop=loop), iterables)
    return map(callback, *await asyncio.gather(*futures, loop=loop))


def map_async(callback, *iterables, loop=None):
    if any(map(inspect.isawaitable, iterables)):
        return _map_async(callback, *iterables, loop=loop)
    return map(callback, *iterables)