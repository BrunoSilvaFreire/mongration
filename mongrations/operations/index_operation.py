from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.source import CollectionSource
from mongrations.misc.not_supported import NotSupported
from mongrations.operations.operation import Operation


class IndexOperation(Operation):
    def __init__(self, index: dict):
        super().__init__()
        self.index = index

    def accepts_dependency_output(self, phase, destination):
        raise NotSupported()

    def create_default_destination(self, phase):
        raise NotSupported()

    async def invoke(self, client: AsyncIOMotorClient, progress: tqdm, phase):
        src = phase.source()
        progress.total = 1
        progress.refresh()
        if isinstance(src, CollectionSource):
            database = src.database
            collection = src.collection
        else:
            raise Exception(f"Incompatible source for aggregation: {src}. Expected CollectionSource.")
        collection = client.get_database(database).get_collection(collection)
        index_name = await collection.create_index(self.index)
        progress.update()

        return 1

    def needs_destination(self):
        return False
