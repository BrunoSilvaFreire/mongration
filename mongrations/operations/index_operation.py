from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm import tqdm

from mongrations.operations.collection_operation import CollectionOperation


class IndexOperation(CollectionOperation):
    def __init__(self, index: dict, index_type: Optional[str] = None):
        super().__init__()
        self.index = index  # Expected to be a dictionary specifying the index fields and options
        self.index_type = index_type
        self._phase = None

    async def invoke(self, client, progress, phase):
        self._phase = phase
        return await super().invoke(client, progress, phase)

    async def run(self, collection, client: AsyncIOMotorClient, progress: tqdm, phase):
        existing_indexes = await collection.list_indexes().to_list(length=None)
        index_fields = list(self.index.keys())

        formatted_index = [(field, self.index[field]) for field in index_fields]

        index_exists = any(
            set(index['key'].items()) == set(formatted_index)
            for index in existing_indexes
        )

        if not index_exists:
            if self.index_type:
                await collection.create_index([(self.index, self.index_type)])
            else:
                index_name = await collection.create_index(self.index)

    def __str__(self):
        fields = ', '.join([f"{k}" for k in self.index.keys()])
        if self._phase:
            collection_info = self._format_collection_info(self._phase)
            return f"Create Index on {collection_info} ({fields})"
        return f"Create Index ({fields})"