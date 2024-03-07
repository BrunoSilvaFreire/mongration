from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.operations.collection_operation import CollectionOperation


class IndexOperation(CollectionOperation):
    def __init__(self, index: dict):
        super().__init__()
        self.index = index  # Expected to be a dictionary specifying the index fields and options

    async def run(self, collection, client: AsyncIOMotorClient, progress: tqdm, phase):
        existing_indexes = await collection.list_indexes().to_list(length=None)
        index_fields = list(self.index.keys())

        formatted_index = [(field, self.index[field]) for field in index_fields]

        index_exists = any(
            set(index['key'].items()) == set(formatted_index)
            for index in existing_indexes
        )

        if not index_exists:
            index_name = await collection.create_index(self.index)