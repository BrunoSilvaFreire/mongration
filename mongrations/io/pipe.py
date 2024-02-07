import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

from mongrations.io.destination import Destination
from mongrations.io.source import Source


class Pipe(Source, Destination):
    def __init__(self):
        self._queue = asyncio.Queue()
        self._end_of_pipe = False
        self._total_hint = None

    async def push(self, item):
        await self._queue.put(item)

    async def close(self):
        self._end_of_pipe = True
        # Add a sentinel value to unblock the cursor if it's waiting for items.
        await self._queue.put(None)

    async def cursor(self, client: AsyncIOMotorClient):
        while self._total_hint is None:
            await asyncio.sleep(0)
        return self._cursor(), self._total_hint

    async def _cursor(self):
        while not (self._end_of_pipe and self._queue.empty()):
            item = await self._queue.get()
            if item is None:
                break
            yield item

    def __str__(self):
        return "Pipe"

    async def hint_total(self, estimated_total):
        self._total_hint = estimated_total
