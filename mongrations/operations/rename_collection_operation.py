from mongrations.operations.collection_operation import CollectionOperation


class RenameCollectionOperation(CollectionOperation):
    def __init__(self, new_name: str):
        super().__init__()
        self._new_name = new_name
        self._phase = None

    async def invoke(self, client, progress, phase):
        self._phase = phase
        return await super().invoke(client, progress, phase)

    async def run(self, collection, client, progress, phase):
        await collection.rename(self._new_name)

    def __str__(self):
        if self._phase:
            collection_info = self._format_collection_info(self._phase)
            return f"Rename {collection_info} → {self._new_name}"
        return f"Rename Collection → {self._new_name}"

