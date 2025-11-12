from mongrations.operations.collection_operation import CollectionOperation
from mongrations.io.source import CollectionSource


class DeleteCollectionOperation(CollectionOperation):
    def __init__(self, database: str, collection: str):
        super().__init__()
        self._database = database
        self._collection = collection
        self._phase = None

    async def invoke(self, client, progress, phase):
        self._phase = phase
        return await super().invoke(client, progress, phase)

    async def run(self, collection, client, progress, phase):
        await collection.drop()

    def __str__(self):
        return f"Delete Collection ({self._database}.{self._collection})"
