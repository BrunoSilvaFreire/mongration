import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.io.destination import Destination
from mongrations.io.source import DocumentSource


class Pipe(DocumentSource, Destination):
    def __init__(self):
        self._queue = asyncio.Queue()
        self._end_of_pipe = False
        self._has_been_hinted = False
        self._hinted_future = asyncio.Future()
        self._total_hint = None

    async def push(self, item):
        await self._queue.put(item)

    async def close(self):
        self._end_of_pipe = True
        # Add a sentinel value to unblock the cursor if it's waiting for items.
        await self._queue.put(None)

    async def cursor(self, client: AsyncIOMotorClient):
        if not self._has_been_hinted:
            self._total_hint = await self._hinted_future
        return self._cursor(), self._total_hint

    async def _cursor(self):
        while not (self._end_of_pipe and self._queue.empty()):
            item = await self._queue.get()
            if item is None:
                break
            yield item

    def pipe_into(self, src, dst):
        src._destination = self
        dst._source = self

    def __str__(self):
        return "Pipe"

    def hint_total(self, estimated_total):
        self._has_been_hinted = True
        self._hinted_future.set_result(estimated_total)
