from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.source import Source


class Operation:
    def invoke(self, client: AsyncIOMotorClient, progress: tqdm, source: Source):
        raise Exception("Not implemented")


class PythonOperation(Operation):
    def __init__(self, block):
        self._block = block

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, source: Source):
        progress.set_description(f"Getting cursor for {source}.")
        cursor, estimated_total = await source.cursor(client)
        progress.total = estimated_total
        async for doc in cursor:
            progress.set_description(str(doc['_id']))
            yield self._block(doc)
            progress.update()

    def __str__(self):
        return self._block.__name__
