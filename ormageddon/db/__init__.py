import contextlib

import peewee
import tasklocals


class TaskConnectionLocal(peewee._BaseConnectionLocal, tasklocals.local):

    def __init__(self, **kwargs):
        # ignore asyncio current task absence
        with contextlib.suppress(RuntimeError):
            super().__init__()
