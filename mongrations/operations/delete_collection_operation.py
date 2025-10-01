from mongrations.operations.collection_operation import CollectionOperation
from mongrations.io.source import CollectionSource


class DeleteCollectionOperation(CollectionOperation):
    def __init__(self):
        super().__init__()
        self._phase = None

    async def invoke(self, client, progress, phase):
        self._phase = phase
        return await super().invoke(client, progress, phase)

    async def run(self, collection, client, progress, phase):
        await collection.drop()

    def __str__(self):
        if self._phase:
            collection_info = self._format_collection_info(self._phase)
            return f"Delete Collection ({collection_info})"
        return "Delete Collection"
