import asyncio

from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.destination import Destination
from mongrations.io.source import Source
from mongrations.operations.operation import Operation


class PythonOperation(Operation):
    def __init__(self, block):
        self._block = block

    async def _process(self, cursor, progress):
        async for doc in cursor:
            yield self._block(doc)
            progress.update()

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        batch_size = 64

        source = phase.source()
        destination = phase.destination()

        cursor, estimated_total = await source.cursor(client)
        progress.total = estimated_total

        destination.hint_total(estimated_total)

        sum = 0
        current_batch = 0
        if destination is None:
            async for _ in self._process(cursor, progress):
                current_batch = await self._notify_batch(batch_size, current_batch)
                sum += 1
        else:
            async for new_doc in self._process(cursor, progress):
                await destination.push(new_doc)
                current_batch = await self._notify_batch(batch_size, current_batch)
                sum += 1

        return sum

    async def _notify_batch(self, batch_size, current_batch):
        if current_batch > batch_size:
            await asyncio.sleep(0)
            return 0
        else:
            return current_batch + 1

    def __str__(self):
        return self._block.__name__
