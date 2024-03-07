from mongrations.operations.collection_operation import CollectionOperation


class RenameCollectionOperation(CollectionOperation):
    def __init__(self, new_name: str):
        super().__init__()
        self._new_name = new_name

    async def run(self, collection, client, progress, phase):
        await collection.rename(self._new_name)

