import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.pipe import Pipe
from mongrations.operations.operation import Operation


class AbstractPythonOperation(Operation):
    def __init__(self, block):
        super().__init__()
        self._block = block

    async def _process(self, cursor, progress):
        async for doc in cursor:
            yield self._block(doc)
            progress.update()

    def create_default_destination(self, phase):
        return Pipe()

    def _setup(self, client, phase, progress):
        raise NotImplemented()

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        batch_size = 64

        destination = phase.destination()

        increment = 0
        current_batch = 0
        iterator = self._setup(client, phase, progress)
        if destination is None:
            async for _ in iterator:
                current_batch = await self._notify_batch(batch_size, current_batch)
                increment += 1
        else:
            async for new_doc in iterator:

                if new_doc is not None:
                    await destination.push(new_doc)
                current_batch = await self._notify_batch(batch_size, current_batch)
                increment += 1

        return increment

    async def _notify_batch(self, batch_size, current_batch):
        if current_batch > batch_size:
            await asyncio.sleep(0)
            return 0
        else:
            return current_batch + 1

    def __str__(self):
        return self._block.__name__


class DocumentPythonOperation(AbstractPythonOperation):
    def __init__(self, block):
        super().__init__(block)

    async def _setup(self, client, phase, progress):
        source = phase.source()
        destination = phase.destination()
        cursor, estimated_total = await source.cursor(client)
        progress.total = estimated_total

        destination.hint_total(estimated_total)
        async for doc in cursor:
            yield self._block(doc)
            progress.update()
