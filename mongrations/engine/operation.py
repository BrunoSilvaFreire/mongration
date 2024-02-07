from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.pipe import Pipe
from mongrations.io.source import Source


class Operation:
    def invoke(self, client: AsyncIOMotorClient, progress: tqdm, source: Source, optional_pipe: Pipe):
        raise Exception("Not implemented")


class PythonOperation(Operation):
    def __init__(self, block):
        self._block = block

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, source: Source, optional_pipe: Pipe):
        cursor, estimated_total = await source.cursor(client)
        progress.total = estimated_total
        if optional_pipe is not None:
            await optional_pipe.hint_total(estimated_total)
        async for doc in cursor:
            yield self._block(doc)
            progress.update()

    def __str__(self):
        return self._block.__name__
