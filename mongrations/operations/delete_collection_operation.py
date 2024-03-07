from mongrations.operations.collection_operation import CollectionOperation


class DeleteCollectionOperation(CollectionOperation):
    async def run(self, collection, client, progress, phase):
        await collection.drop()
