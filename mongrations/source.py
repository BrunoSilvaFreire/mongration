from motor.motor_asyncio import AsyncIOMotorClient


class Source:
    pass

    def cursor(self, client: AsyncIOMotorClient):
        pass


class PhaseSource(Source):
    def __init__(self, source):
        self._source = source

    async def cursor(self, client: AsyncIOMotorClient):
        pass


class CollectionSource(Source):
    def __init__(self, database: str, collection: str, filter: dict):
        self._database = database
        self._collection = collection
        self._filter = filter

    async def cursor(self, client: AsyncIOMotorClient):
        collection = client.get_database(self._database).get_collection(self._collection)
        return collection.find(filter=self._filter), await collection.estimated_document_count(maxTimeMS=5 * 1000)

    def __str__(self):
        return f"{self._database}/{self._collection}"
