from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.source import CollectionSource
from mongrations.misc.not_supported import NotSupported
from mongrations.operations.operation import Operation


class CollectionOperation(Operation):

    def accepts_dependency_output(self, phase, destination):
        raise NotSupported()

    def create_default_destination(self, phase):
        raise NotSupported()

    def needs_destination(self):
        return False

    async def run(self, collection, client: AsyncIOMotorClient, progress: tqdm, phase):
        pass

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        src = phase.source()
        progress.total = 1
        progress.refresh()
        if isinstance(src, CollectionSource):
            database = src.database
            collection = src.collection
        else:
            raise Exception(f"Incompatible source for collection operation: {src}. Expected CollectionSource.")

        # Access the specified collection
        collection = client.get_database(database).get_collection(collection)
        result = await self.run(collection, client, progress, phase)
        progress.update()

        return result or 1