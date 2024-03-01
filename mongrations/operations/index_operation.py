from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.io.source import CollectionSource
from mongrations.misc.not_supported import NotSupported
from mongrations.operations.operation import Operation


class IndexOperation(Operation):
    def __init__(self, index: dict):
        super().__init__()
        self.index = index  # Expected to be a dictionary specifying the index fields and options

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
            raise Exception(f"Incompatible source for index operation: {src}. Expected CollectionSource.")

        # Access the specified collection
        collection = client.get_database(database).get_collection(collection)

        # Check if a compatible index already exists
        existing_indexes = await collection.list_indexes().to_list(length=None)
        index_fields = list(self.index.keys())

        # Convert index specification to a format that can be compared with existing indexes
        formatted_index = [(field, self.index[field]) for field in index_fields]

        index_exists = any(
            set(index['key'].items()) == set(formatted_index)
            for index in existing_indexes
        )

        # If the index does not exist, create it
        if not index_exists:
            index_name = await collection.create_index(self.index)
        progress.update()

        return 1

    def needs_destination(self):
        return False
